using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using System;
using System.Data;
using System.Diagnostics;

[ApiController]
[Route("api/[controller]")]
public class IRISController : ControllerBase
{
    private readonly string tidbConnStr = "Server=127.0.0.1;Port=4000;User ID=root;Password=;Database=test;Allow User Variables=true";
    private readonly string sqlServerConnStr = "Server=127.0.0.1,1433;User ID=sa;Password=YourStrong!Passw0rd;Database=master;TrustServerCertificate=True;";
    private readonly string crdbConnStr = "Host=127.0.0.1;Port=26257;Username=root;Password=;Database=test;SSL Mode=Disable";

    [HttpGet("tidb")]
    public async Task<IActionResult> GetTidb()
    {
        var random = new Random();
        int userId = random.Next(0, 1001);

        string[] reasons = { "Nap tien", "Rut tien" };
        string reason = reasons[random.Next(reasons.Length)];

        decimal absAmount = (decimal)(random.NextDouble() * 1000);
        decimal changeAmount = reason == "Nap tien" ? absAmount : -absAmount;

        using (var connection = new MySqlConnection(tidbConnStr))
        {
            await connection.OpenAsync();

            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    decimal currentBalance;
                    using (var checkCmd = new MySqlCommand(
                        @"SELECT Balance
                      FROM Balances
                      WHERE UserId = @UserId
                      FOR UPDATE;", connection, transaction))
                    {
                        //checkCmd.Prepare();
                        checkCmd.Parameters.AddWithValue("@UserId", userId);
                        var result = await checkCmd.ExecuteScalarAsync();
                        if (result == null)
                        {
                            return NotFound(new { message = $"Không tìm thấy UserId={userId} trong Balances." });
                        }
                        currentBalance = (decimal)result;
                    }

                    decimal newBalance = currentBalance + changeAmount;

                    using (var combinedCmd = new MySqlCommand(
                        @"UPDATE Balances
                      SET Balance = @NewBalance,
                          LastUpdatedAt = NOW()
                      WHERE UserId = @UserId
                        AND Balance >= @MinBalanceRequired;

                      INSERT INTO BalanceHistory
                        (UserId, ChangeAmount, BalanceBefore, BalanceAfter, Reason)
                      VALUES
                        (@UserId, @ChangeAmount, @BalanceBefore, @BalanceAfter, @Reason);", connection, transaction))
                    {
                        //combinedCmd.Prepare();
                        combinedCmd.Parameters.AddWithValue("@NewBalance", newBalance);
                        combinedCmd.Parameters.AddWithValue("@UserId", userId);
                        combinedCmd.Parameters.AddWithValue("@ChangeAmount", changeAmount);
                        combinedCmd.Parameters.AddWithValue("@BalanceBefore", currentBalance);
                        combinedCmd.Parameters.AddWithValue("@BalanceAfter", newBalance);

                        decimal minBalanceRequired = changeAmount < 0 ? Math.Abs(changeAmount) : 0;
                        combinedCmd.Parameters.AddWithValue("@MinBalanceRequired", minBalanceRequired);
                        combinedCmd.Parameters.AddWithValue("@Reason", reason);

                        int rowsAffected = await combinedCmd.ExecuteNonQueryAsync();

                        if (rowsAffected < 2)
                        {
                            await transaction.RollbackAsync();
                            return BadRequest(new { message = $"Không đủ số dư: ChangeAmount={changeAmount}, Balance={currentBalance}" });
                        }

                        await transaction.CommitAsync();
                    }
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    return StatusCode(500, new { message = $"[TiDB] Error in UserId={userId}", detail = ex.Message });
                }
            }

            return Ok(new
            {
                message = $"[TiDB] Updated balance: UserId={userId}, ChangeAmount={changeAmount}, Reason='{reason}'"
            });
        }
    }
    [HttpGet("tidbnew")]
    public async Task<IActionResult> GetTidbOptimized()
    {
        var random = new Random();
        int userId = random.Next(0, 1000);
        string reason = random.NextDouble() > 0.5 ? "Nap tien" : "Rut tien";
        decimal changeAmount = reason == "Nap tien"
            ? (decimal)(random.NextDouble() * 1000)
            : -(decimal)(random.NextDouble() * 1000);

        try
        {
            await using var connection = new MySqlConnection(tidbConnStr);
            await connection.OpenAsync();

            await using var transaction = await connection.BeginTransactionAsync(
                IsolationLevel.ReadCommitted);

            decimal currentBalance;
            using (var checkCmd = new MySqlCommand(
                @"SELECT Balance FROM Balances WHERE UserId = @UserId FOR UPDATE",
                connection, transaction))
            {
                checkCmd.Parameters.AddWithValue("@UserId", userId);
                var result = await checkCmd.ExecuteScalarAsync();

                if (result == null)
                {
                    return NotFound(new { message = $"User {userId} not found" });
                }
                currentBalance = (decimal)result;
            }

            using (var updateCmd = new MySqlCommand(
                @"UPDATE Balances
              SET Balance = Balance + @ChangeAmount,
                  LastUpdatedAt = NOW()
              WHERE UserId = @UserId
              AND Balance >= CASE WHEN @ChangeAmount < 0 THEN ABS(@ChangeAmount) ELSE 0 END",
                connection, transaction))
            {
                updateCmd.Parameters.AddWithValue("@ChangeAmount", changeAmount);
                updateCmd.Parameters.AddWithValue("@UserId", userId);

                int affectedRows = await updateCmd.ExecuteNonQueryAsync();
                if (affectedRows == 0)
                {
                    await transaction.RollbackAsync();
                    return BadRequest(new
                    {
                        success = false,
                        error = "Insufficient balance or account not found"
                    });
                }
            }

            decimal newBalance = currentBalance + changeAmount;

            using (var historyCmd = new MySqlCommand(
                @"INSERT INTO BalanceHistory
              (UserId, ChangeAmount, BalanceBefore, BalanceAfter, Reason)
              VALUES (@UserId, @ChangeAmount, @BalanceBefore, @BalanceAfter, @Reason)",
                connection, transaction))
            {
                historyCmd.Parameters.AddWithValue("@UserId", userId);
                historyCmd.Parameters.AddWithValue("@ChangeAmount", changeAmount);
                historyCmd.Parameters.AddWithValue("@BalanceBefore", currentBalance);
                historyCmd.Parameters.AddWithValue("@BalanceAfter", newBalance);
                historyCmd.Parameters.AddWithValue("@Reason", reason);

                await historyCmd.ExecuteNonQueryAsync();
            }

            await transaction.CommitAsync();

            return Ok(new
            {
                message = $"[TiDB] Updated balance: UserId={userId}, ChangeAmount={changeAmount}, Reason='{reason}'"

            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new
            {
                success = false,
                error = "System error",
                details = ex.Message
            });
        }
    }

    // Sử dụng PS
    [HttpGet("tidbps")]
    public async Task<IActionResult> GetTidbps()
    {
        var random = new Random();
        int userId = random.Next(0, 1001);
        string[] reasons = { "Nap tien", "Rut tien" };
        string reason = reasons[random.Next(reasons.Length)];
        decimal absAmount = (decimal)(random.NextDouble() * 1000);
        decimal changeAmount = reason == "Nap tien" ? absAmount : -absAmount;

        using var connection = new MySqlConnection(tidbConnStr);
        await connection.OpenAsync();
        using var transaction = await connection.BeginTransactionAsync();

        try
        {
            await using (var prepareSelectCmd = new MySqlCommand(
                "PREPARE checkStmt FROM 'SELECT Balance FROM Balances WHERE UserId = ? FOR UPDATE';",
                connection, transaction))
            {
                await prepareSelectCmd.ExecuteNonQueryAsync();
            }

            await using (var setUidCmd = new MySqlCommand("SET @uid = @UserId;", connection, transaction))
            {
                setUidCmd.Parameters.AddWithValue("@UserId", userId);
                await setUidCmd.ExecuteNonQueryAsync();
            }

            decimal currentBalance;
            await using (var execCheckCmd = new MySqlCommand("EXECUTE checkStmt USING @uid;", connection, transaction))
            await using (var reader = await execCheckCmd.ExecuteReaderAsync())
            {
                if (!await reader.ReadAsync())
                {
                    return NotFound(new { message = $"Không tìm thấy UserId={userId} trong Balances." });
                }
                currentBalance = reader.GetDecimal(0);
            }

            await using (var deallocCheck = new MySqlCommand("DEALLOCATE PREPARE checkStmt;", connection, transaction))
            {
                await deallocCheck.ExecuteNonQueryAsync();
            }

            decimal newBalance = currentBalance + changeAmount;
            decimal minBalance = changeAmount < 0 ? Math.Abs(changeAmount) : 0;
            DateTime now = DateTime.UtcNow;

            string updateSql = @"
                UPDATE Balances
                SET Balance = ?, LastUpdatedAt = ?
                WHERE UserId = ? AND Balance >= ?;
            ";

            await using (var prepareUpdate = new MySqlCommand("PREPARE updateStmt FROM @sql;", connection, transaction))
            {
                prepareUpdate.Parameters.AddWithValue("@sql", updateSql);
                await prepareUpdate.ExecuteNonQueryAsync();
            }

            await using (var setUpdateVars = new MySqlCommand(@"
                SET 
                    @b = @NewBalance,
                    @t = @Timestamp,
                    @u = @UserId,
                    @m = @MinBalance;
            ", connection, transaction))
            {
                setUpdateVars.Parameters.AddWithValue("@NewBalance", newBalance);
                setUpdateVars.Parameters.AddWithValue("@Timestamp", now);
                setUpdateVars.Parameters.AddWithValue("@UserId", userId);
                setUpdateVars.Parameters.AddWithValue("@MinBalance", minBalance);
                await setUpdateVars.ExecuteNonQueryAsync();
            }

            int rowsAffected;
            await using (var execUpdate = new MySqlCommand(
                "EXECUTE updateStmt USING @b, @t, @u, @m;", connection, transaction))
            {
                rowsAffected = await execUpdate.ExecuteNonQueryAsync();
            }

            await using (var deallocUpdate = new MySqlCommand("DEALLOCATE PREPARE updateStmt;", connection, transaction))
            {
                await deallocUpdate.ExecuteNonQueryAsync();
            }

            if (rowsAffected == 0)
            {
                await transaction.RollbackAsync();
                return BadRequest(new
                {
                    message = $"Không đủ số dư: ChangeAmount={changeAmount}, Balance={currentBalance}"
                });
            }

            string insertSql = @"
                INSERT INTO BalanceHistory
                    (UserId, ChangeAmount, BalanceBefore, BalanceAfter, Reason)
                VALUES
                    (?, ?, ?, ?, ?);
            ";

            await using (var prepareInsert = new MySqlCommand("PREPARE insertStmt FROM @sql;", connection, transaction))
            {
                prepareInsert.Parameters.AddWithValue("@sql", insertSql);
                await prepareInsert.ExecuteNonQueryAsync();
            }

            await using (var setInsertVars = new MySqlCommand(@"
                SET 
                    @u = @UserId,
                    @c = @ChangeAmount,
                    @bf = @BalanceBefore,
                    @af = @BalanceAfter,
                    @r = @Reason;
            ", connection, transaction))
            {
                setInsertVars.Parameters.AddWithValue("@UserId", userId);
                setInsertVars.Parameters.AddWithValue("@ChangeAmount", changeAmount);
                setInsertVars.Parameters.AddWithValue("@BalanceBefore", currentBalance);
                setInsertVars.Parameters.AddWithValue("@BalanceAfter", newBalance);
                setInsertVars.Parameters.AddWithValue("@Reason", reason);
                await setInsertVars.ExecuteNonQueryAsync();
            }

            await using (var execInsert = new MySqlCommand(
                "EXECUTE insertStmt USING @u, @c, @bf, @af, @r;", connection, transaction))
            {
                await execInsert.ExecuteNonQueryAsync();
            }

            await using (var deallocInsert = new MySqlCommand("DEALLOCATE PREPARE insertStmt;", connection, transaction))
            {
                await deallocInsert.ExecuteNonQueryAsync();
            }

            await transaction.CommitAsync();

            return Ok(new
            {
                message = $"[TiDB] Updated balance thành công: UserId={userId}, Amount={changeAmount}, Reason='{reason}'"
            });
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            return StatusCode(500, new
            {
                message = $"[TiDB] Lỗi trong quá trình xử lý UserId={userId}",
                detail = ex.Message
            });
        }
    }
    // Sử dụng rawSQL
    [HttpGet("sqlserver")]
    public async Task<IActionResult> GetSqlServer()
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
                                return NotFound(new { message = $"Không tìm thấy UserId={userId} trong Balances." });
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
                                return BadRequest(new { message = $"Không đủ số dư: ChangeAmount={changeAmount}, Balance={currentBalance}" });
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
                        return StatusCode(500, new { message = $"[SQL Server] Error in UserId={userId}", detail = ex.Message });
                    }
                }

                return Ok(new
                {
                    message = $"[SQL Server] Updated balance: UserId={userId}, ChangeAmount={changeAmount}, Reason='{reason}'"
                });
            }

        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Có lỗi xảy ra khi cập nhật balance.", details = ex.Message });
        }
    }
    //Sử dụng stored procedure
    [HttpGet("sqlserversp")]
    public async Task<IActionResult> GetSqlServerWithSP()
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
                        return NotFound(new { message = $"Không tìm thấy UserId={userId} trong Balances." });
                    }
                    currentBalance = (decimal)result;
                }

                if (Math.Abs(changeAmount) > currentBalance)
                {
                    return BadRequest(new { message = $"Rút tiền vượt quá số dư hiện tại: ChangeAmount={changeAmount}, Balance={currentBalance}" });
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

                return Ok(new { message = $"[SQL Server] Updated balance: UserId={userId}, ChangeAmount={changeAmount}, BalanceBefore={currentBalance}, BalanceAfter={newBalance}, Reason='{reason}'" });
            }

        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Có lỗi xảy ra khi cập nhật balance.", details = ex.Message });
        }
    }
    [HttpGet("crdb")]
    public async Task<IActionResult> GetCockroach()
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
                            return NotFound(new { message = $"Không tìm thấy UserId={userId} trong Balances." });
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
                            return BadRequest(new { message = "Không đủ số dư hoặc không tìm thấy người dùng." });
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

                    return Ok(new
                    {
                        message = $"[CockroachDB] Updated balance: UserId={userId}, ChangeAmount={changeAmount}, Reason='{reason}'"
                    });
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    return StatusCode(500, new { message = $"[CockroachDB] Error in UserId={userId}", detail = ex.Message });
                }
            }
        }

    }
    [HttpGet("active")]
    public async Task<IActionResult> Active()
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

            return Ok(new
            {
                Message = "Ghi đồng thời thành công",
                DC_Result = dcResult,
                DR_Result = drResult,
                FinalBalanceDC = balanceDC,
                FinalBalanceDR = balanceDR
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new
            {
                Message = "Ghi đồng thời thất bại",
                Error = ex.Message,
                InnerErrors = new
                {
                    DC_Error = dcTask.Exception?.Message,
                    DR_Error = drTask.Exception?.Message
                }
            });
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
                        return NotFound(new { message = $"Không tìm thấy UserId={userId} trong Balances." });
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
