using Npgsql;

namespace TestTiDB.Services
{
    public class CRHandler : ICRHandler
    {
        private readonly string crdbConnStr = "Host=127.0.0.1;Port=26257;Username=root;Password=;Database=test;SSL Mode=Disable";

        public async Task<HandlerResult> Get()
        {
            var random = new Random();
            int userId = random.Next(0, 1001);

            string[] reasons = { "Nap tien", "Rut tien" };
            string reason = reasons[random.Next(reasons.Length)];

            decimal absAmount = (decimal)(random.NextDouble() * 1000);
            decimal changeAmount = reason == "Nap tien" ? absAmount : -absAmount;

            using (var connection = new NpgsqlConnection(crdbConnStr))
            {
                await connection.OpenAsync();

                using (var transaction = await connection.BeginTransactionAsync())
                {
                    try
                    {
                        decimal currentBalance;
                        using (var checkCmd = new NpgsqlCommand(
                            @"SELECT Balance FROM Balances WHERE UserId = @UserId FOR UPDATE;", connection, transaction))
                        {
                            checkCmd.Parameters.AddWithValue("@UserId", userId);
                            var result = await checkCmd.ExecuteScalarAsync();
                            if (result == null)
                            {
                                return new HandlerResult
                                {
                                    StatusCode = 404,
                                    Message = $"Không tìm thấy UserId={userId} trong Balances."
                                };
                            }
                            currentBalance = (decimal)result;
                        }

                        decimal newBalance = currentBalance + changeAmount;
                        decimal minBalanceRequired = changeAmount < 0 ? Math.Abs(changeAmount) : 0;

                        using (var updateCmd = new NpgsqlCommand(
                            @"UPDATE Balances
                  SET Balance = @NewBalance,
                      LastUpdatedAt = now()
                  WHERE UserId = @UserId
                    AND Balance >= @MinBalanceRequired;", connection, transaction))
                        {
                            updateCmd.Parameters.AddWithValue("@NewBalance", newBalance);
                            updateCmd.Parameters.AddWithValue("@UserId", userId);
                            updateCmd.Parameters.AddWithValue("@MinBalanceRequired", minBalanceRequired);

                            int rowsUpdated = await updateCmd.ExecuteNonQueryAsync();
                            if (rowsUpdated == 0)
                            {
                                await transaction.RollbackAsync();
                                return new HandlerResult
                                {
                                    StatusCode = 400,
                                    Message = "Không đủ số dư hoặc không tìm thấy người dùng."
                                };
                            }
                        }

                        using (var insertCmd = new NpgsqlCommand(
                            @"INSERT INTO BalanceHistory
                  (UserId, ChangeAmount, BalanceBefore, BalanceAfter, Reason, region_name)
                  VALUES
                  (@UserId, @ChangeAmount, @BalanceBefore, @BalanceAfter, @Reason, @RegionName);", connection, transaction))
                        {
                            insertCmd.Parameters.AddWithValue("@UserId", userId);
                            insertCmd.Parameters.AddWithValue("@ChangeAmount", changeAmount);
                            insertCmd.Parameters.AddWithValue("@BalanceBefore", currentBalance);
                            insertCmd.Parameters.AddWithValue("@BalanceAfter", newBalance);
                            insertCmd.Parameters.AddWithValue("@Reason", reason);
                            insertCmd.Parameters.AddWithValue("@RegionName", "dr");
                            await insertCmd.ExecuteNonQueryAsync();
                        }

                        await transaction.CommitAsync();

                        return new HandlerResult
                        {
                            StatusCode = 200,
                            Message = $"[CRDB] Updated balance: UserId={userId}, ChangeAmount={changeAmount}, Reason='{reason}'"
                        };
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync();
                        return new HandlerResult
                        {
                            StatusCode = 500,
                            Message = $"[CockroachDB] Error in UserId={userId}" + ex.Message
                        }
                    ;
                    }
                }
            }
        }

        public async Task<HandlerResult> Active()
        {
            var random = new Random();
            long userId = random.Next(0, 1001);
            string reason = "Giao dịch đa vùng";

            var dcTask = ProcessTransactionInRegion("dc", userId, -100, reason);
            var drTask = ProcessTransactionInRegion("dr", userId, -50, reason);

            try
            {
                await Task.WhenAll(dcTask, drTask);

                var dcResult = await dcTask;
                var drResult = await drTask;
                var (balanceDC, balanceDR) = await GetCurrentBalanceFromBothNodes(userId);

                return new HandlerResult
                {
                    StatusCode = 200,
                    Data = new
                    {
                        Message = "Ghi đồng thời thành công",
                        DC_Result = dcResult,
                        DR_Result = drResult,
                        FinalBalanceDC = balanceDC,
                        FinalBalanceDR = balanceDR
                    }
                };
            }
            catch (Exception ex)
            {
                return new HandlerResult
                {
                    StatusCode = 500,
                    Data = new
                    {
                        Message = "Ghi đồng thời thất bại",
                        Error = ex.Message,
                        InnerErrors = new
                        {
                            DC_Error = dcTask?.Exception?.Message,
                            DR_Error = drTask?.Exception?.Message
                        }
                    }
                };
            }
        }
        private async Task<object> ProcessTransactionInRegion(string region, long userId, decimal changeAmount, string reason)
        {
            var node = region == "dc"
                ? ("localhost", 26257)
                : ("localhost", 26259);



            string connStr = $"Host={node.Item1};Port={node.Item2};Database=test;Username=root;SSL Mode=Disable";

            using var connection = new NpgsqlConnection(connStr);
            await connection.OpenAsync();

            var retryCount = 0;
            while (retryCount < 3)
            {
                try
                {
                    decimal currentBalance = 0;
                    using var transaction = await connection.BeginTransactionAsync();
                    using (var checkCmd = new NpgsqlCommand(
                    @"SELECT Balance FROM Balances WHERE UserId = @UserId FOR UPDATE;", connection, transaction))
                    {
                        checkCmd.Parameters.AddWithValue("@UserId", userId);
                        var result = await checkCmd.ExecuteScalarAsync();
                        if (result == null)
                        {
                            return new HandlerResult { StatusCode = 404, Message = $"Không tìm thấy UserId={userId} trong Balances." };
                        }
                        currentBalance = (decimal)result;
                    }
                    decimal newBalance = currentBalance + changeAmount;
                    decimal minBalance = 0;

                    var updateCmd = new NpgsqlCommand(@"
                UPDATE balances 
                SET balance = balance + @Change,
                    lastupdatedat = now()
                WHERE userid = @UserId 
                AND balance + @Change >= @MinBalance", connection, transaction);

                    updateCmd.Parameters.AddWithValue("@UserId", userId);
                    updateCmd.Parameters.AddWithValue("@Change", changeAmount);
                    updateCmd.Parameters.AddWithValue("@MinBalance", minBalance);

                    int affected = await updateCmd.ExecuteNonQueryAsync();
                    if (affected == 0)
                    {
                        await transaction.RollbackAsync();
                        throw new Exception("Không đủ số dư hoặc user không tồn tại");
                    }

                    var historyCmd = new NpgsqlCommand(@"
                INSERT INTO balancehistory
                (userid, changeamount, balancebefore, balanceafter, reason, crdb_region)
                VALUES (@UserId, @Change, @Before, @After, @Reason, @Region)", connection, transaction);

                    historyCmd.Parameters.AddWithValue("@UserId", userId);
                    historyCmd.Parameters.AddWithValue("@Change", changeAmount);
                    historyCmd.Parameters.AddWithValue("@Before", currentBalance);
                    historyCmd.Parameters.AddWithValue("@After", newBalance);
                    historyCmd.Parameters.AddWithValue("@Reason", reason);
                    historyCmd.Parameters.AddWithValue("@Region", region);
                    await historyCmd.ExecuteNonQueryAsync();

                    await transaction.CommitAsync();

                    return new
                    {
                        Region = region,
                        UserId = userId,
                        OldBalance = currentBalance,
                        ChangeAmount = changeAmount,
                        NewBalance = newBalance,
                        Status = "Thành công"
                    };
                }
                catch (PostgresException ex) when (ex.SqlState == "40001")
                {
                    retryCount++;
                    if (retryCount >= 3) throw;
                    await Task.Delay(100 * retryCount);
                }
            }
            throw new Exception("Vượt quá số lần thử lại");
        }


        private async Task<(decimal balance1, decimal balance2)> GetCurrentBalanceFromBothNodes(long userId)
        {
            var connStr1 = "Host=localhost;Port=26257;Database=test;Username=root;SSL Mode=Disable";
            var connStr2 = "Host=localhost;Port=26259;Database=test;Username=root;SSL Mode=Disable";

            var task1 = GetBalanceFromNode(connStr1, userId);
            var task2 = GetBalanceFromNode(connStr2, userId);

            await Task.WhenAll(task1, task2);

            return (task1.Result, task2.Result);
        }

        private async Task<decimal> GetBalanceFromNode(string connectionString, long userId)
        {
            using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            using var cmd = new NpgsqlCommand("SELECT balance FROM balances WHERE userid = @UserId", connection);
            cmd.Parameters.AddWithValue("@UserId", userId);

            var result = await cmd.ExecuteScalarAsync();
            return result != null ? Convert.ToDecimal(result) : 0;
        }
    }
    public class HandlerResult<T>
    {
        public int StatusCode { get; set; }
        public string? Message { get; set; }
        public T? Data { get; set; }
    }
}
