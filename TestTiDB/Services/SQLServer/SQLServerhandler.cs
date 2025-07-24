
using Microsoft.Data.SqlClient;
using System.Data;

namespace TestTiDB.Services
{
    public class SQLServerhandler : ISQLServerhandler
    {
        private readonly string sqlServerConnStr = "Server=127.0.0.1,1433;User ID=sa;Password=YourStrong!Passw0rd;Database=master;TrustServerCertificate=True;";
        public async Task<HandlerResult> GetSqlServer()
        {
            try
            {
                var random = new Random();
                int userId = random.Next(0, 1001);

                string[] reasons = { "Nap tien", "Rut tien" };
                string reason = reasons[random.Next(reasons.Length)];

                decimal absAmount = (decimal)(random.NextDouble() * 1000);
                decimal changeAmount = reason == "Nap tien" ? absAmount : -absAmount;
                using (var connection = new SqlConnection(sqlServerConnStr))
                {
                    await connection.OpenAsync();

                    using (var transaction = await connection.BeginTransactionAsync())
                    {
                        try
                        {
                            decimal currentBalance;
                            using (var checkCmd = new SqlCommand(
                                @"SELECT Balance            FROM Balances WITH (UPDLOCK, ROWLOCK)
                              WHERE UserId = @UserId;", connection, (SqlTransaction)transaction))

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
                            using (var updateCmd = new SqlCommand(
                                @"UPDATE Balances
                              SET Balance = @NewBalance,
                                  LastUpdatedAt = GETDATE()
                              WHERE UserId = @UserId
                                AND Balance >= @MinBalanceRequired;", connection, (SqlTransaction)transaction))
                            {
                                updateCmd.Parameters.AddWithValue("@NewBalance", newBalance);
                                updateCmd.Parameters.AddWithValue("@UserId", userId);
                                decimal minBalanceRequired = changeAmount < 0 ? Math.Abs(changeAmount) : 0;
                                updateCmd.Parameters.AddWithValue("@MinBalanceRequired", minBalanceRequired);

                                int rowsAffected = await updateCmd.ExecuteNonQueryAsync();

                                if (rowsAffected <= 0)
                                {
                                    await transaction.RollbackAsync();
                                    return new HandlerResult { StatusCode = 404, Message = $"Không đủ số dư: ChangeAmount={changeAmount}, Balance={currentBalance}" };
                                }
                            }

                            using (var insertCmd = new SqlCommand(
                                @"INSERT INTO BalanceHistory
                                  (UserId, ChangeAmount, BalanceBefore, BalanceAfter, Reason, CreatedAt)
                                VALUES
                                  (@UserId, @ChangeAmount, @BalanceBefore, @BalanceAfter, @Reason, GETDATE());", connection, (SqlTransaction)transaction))
                            {
                                insertCmd.Parameters.AddWithValue("@UserId", userId);
                                insertCmd.Parameters.AddWithValue("@ChangeAmount", changeAmount);
                                insertCmd.Parameters.AddWithValue("@BalanceBefore", currentBalance);
                                insertCmd.Parameters.AddWithValue("@BalanceAfter", newBalance);
                                insertCmd.Parameters.AddWithValue("@Reason", reason ?? string.Empty);

                                await insertCmd.ExecuteNonQueryAsync();
                            }

                            await transaction.CommitAsync();

                        }
                        catch (Exception ex)
                        {
                            await transaction.RollbackAsync();
                            return new HandlerResult
                            {
                                StatusCode = 500,
                                Message = $"[SQLServer] Error in UserId={userId}" + ex.Message,
                            };
                        }
                    }

                    return new HandlerResult
                    {
                        StatusCode = 200,
                        Message = $"[SQL Server] Updated balance: UserId={userId}, ChangeAmount={changeAmount}, Reason='{reason}'"
                    };
                }

            }
            catch (Exception ex)
            {
                return new HandlerResult
                {
                    StatusCode = 500,
                    Message = $"[SQL Server] Error with" + ex.Message,
                };
            }
        }

        public async Task<HandlerResult> GetSqlServerWithSP()
        {
            try
            {
                var random = new Random();
                int userId = random.Next(0, 1001);

                string[] reasons = { "Nap tien", "Rut tien" };
                string reason = reasons[random.Next(reasons.Length)];

                decimal absAmount = (decimal)(random.NextDouble() * 1000);
                decimal changeAmount = reason == "Nap tien" ? absAmount : -absAmount;

                using (var connection = new SqlConnection(sqlServerConnStr))
                {
                    await connection.OpenAsync();

                    decimal currentBalance;
                    using (var checkCmd = new SqlCommand("SELECT Balance FROM Balances WHERE UserId = @UserId", connection))
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

                    if (Math.Abs(changeAmount) > currentBalance)
                    {
                        return new HandlerResult
                        {
                            StatusCode = 400,
                            Message = $"Không đủ số dư: ChangeAmount={changeAmount}, Balance={currentBalance}"
                        };
                    }
                    decimal newBalance = currentBalance - changeAmount;

                    using (var command = new SqlCommand("UpdateUserBalance", connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("@UserId", userId);
                        command.Parameters.AddWithValue("@ChangeAmount", changeAmount);
                        command.Parameters.AddWithValue("@Reason", reason);

                        await command.ExecuteNonQueryAsync();
                    }

                    return new HandlerResult
                    {
                        StatusCode = 200,
                        Message = $"[SQL Server] Updated balance: UserId={userId}, ChangeAmount={changeAmount}, Reason='{reason}'"
                    };
                }

            }
            catch (Exception ex)
            {
                return new HandlerResult
                {
                    StatusCode = 500,
                    Message = $"[SQLSERVER] Error with" + ex.Message,
                };
            }
        }
    }
}
