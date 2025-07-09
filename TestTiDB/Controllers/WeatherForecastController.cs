using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using System.Data;
using System.Diagnostics;

[ApiController]
[Route("api/[controller]")]
public class IRISController : ControllerBase
{
    private readonly string tidbConnStr = "Server=127.0.0.1;Port=4000;User ID=root;Password=;Database=test;Allow User Variables=true";
    private readonly string sqlServerConnStr = "Server=127.0.0.1,1433;User ID=sa;Password=YourStrong!Passw0rd;Database=master;TrustServerCertificate=True;";
    private static readonly object RandomLock = new object();
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
    //Tách command update và insert
    [HttpGet("tidbnew")]
    public async Task<IActionResult> GetTidbNew()
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
                        checkCmd.Parameters.AddWithValue("@UserId", userId);
                        var result = await checkCmd.ExecuteScalarAsync();
                        
                        if (result == null)
                        {
                            return NotFound(new { message = $"Không tìm thấy UserId={userId} trong Balances." });
                        }
                        currentBalance = (decimal)result;
                    }

                    decimal newBalance = currentBalance + changeAmount;

                    using (var updateCmd = new MySqlCommand(
                        @"UPDATE Balances
                            SET Balance = @NewBalance,
                                LastUpdatedAt = NOW()
                          WHERE UserId = @UserId
                            AND Balance >= @MinBalanceRequired;", connection, transaction))
                    {
                        updateCmd.Parameters.AddWithValue("@NewBalance", newBalance);
                        updateCmd.Parameters.AddWithValue("@UserId", userId);
                        decimal minBalanceRequired = 0 - changeAmount;
                        updateCmd.Parameters.AddWithValue("@MinBalanceRequired", minBalanceRequired);
                        await updateCmd.PrepareAsync();

                        int rowsAffected = await updateCmd.ExecuteNonQueryAsync();

                        if (rowsAffected <= 0)
                        {
                            await transaction.RollbackAsync();
                            return BadRequest(new { message = $"Không đủ số dư: ChangeAmount={changeAmount}, Balance={currentBalance}" });
                        }
                    }

                    using (var insertCmd = new MySqlCommand(
                        @"INSERT INTO BalanceHistory
                            (UserId, ChangeAmount, BalanceBefore, BalanceAfter, Reason)
                          VALUES
                            (@UserId, @ChangeAmount, @BalanceBefore, @BalanceAfter, @Reason);", connection, transaction))
                    {
                        insertCmd.Parameters.AddWithValue("@UserId", userId);
                        insertCmd.Parameters.AddWithValue("@ChangeAmount", changeAmount);
                        insertCmd.Parameters.AddWithValue("@BalanceBefore", currentBalance);
                        insertCmd.Parameters.AddWithValue("@BalanceAfter", newBalance);
                        insertCmd.Parameters.AddWithValue("@Reason", reason);

                        await insertCmd.ExecuteNonQueryAsync();
                    }

                    await transaction.CommitAsync();

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
}
