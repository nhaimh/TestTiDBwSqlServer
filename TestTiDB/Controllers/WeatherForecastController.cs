using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using System.Data;
using System.Diagnostics;

[ApiController]
[Route("api/[controller]")]
public class IRISController : ControllerBase
{
    private readonly string tidbConnStr = "Server=127.0.0.1;Port=4000;User ID=root;Password=;Database=test;SslMode=None;UseAffectedRows=True";
    private readonly string sqlServerConnStr = "Server=127.0.0.1,1433;User ID=sa;Password=YourStrong!Passw0rd;Database=master;TrustServerCertificate=True;";
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
                        checkCmd.Parameters.AddWithValue("@UserId", userId);
                        await checkCmd.PrepareAsync();
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
                        combinedCmd.Parameters.AddWithValue("@NewBalance", newBalance);
                        combinedCmd.Parameters.AddWithValue("@UserId", userId);
                        combinedCmd.Parameters.AddWithValue("@ChangeAmount", changeAmount);
                        combinedCmd.Parameters.AddWithValue("@BalanceBefore", currentBalance);
                        combinedCmd.Parameters.AddWithValue("@BalanceAfter", newBalance);

                        decimal minBalanceRequired = changeAmount < 0 ? Math.Abs(changeAmount) : 0;
                        combinedCmd.Parameters.AddWithValue("@MinBalanceRequired", minBalanceRequired);
                        combinedCmd.Parameters.AddWithValue("@Reason", reason);
                        await combinedCmd.PrepareAsync();

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
            using (var cmd = new MySqlCommand(
                "PREPARE checkStmt FROM 'SELECT Balance FROM Balances WHERE UserId = ? FOR UPDATE';", connection, transaction))
            {
                await cmd.ExecuteNonQueryAsync();
            }
            using (var cmd = new MySqlCommand("SET @uid = @UserId;", connection, transaction))
            {
                cmd.Parameters.AddWithValue("@UserId", userId);
                await cmd.ExecuteNonQueryAsync();
            }

            decimal currentBalance;
            using (var cmd = new MySqlCommand("EXECUTE checkStmt USING @uid;", connection, transaction))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                if (!await reader.ReadAsync())
                {
                    return NotFound(new { message = $"Không tìm thấy UserId={userId} trong Balances." });
                }
                currentBalance = reader.GetDecimal(0);
            }
            using (var cmd = new MySqlCommand("DEALLOCATE PREPARE checkStmt;", connection, transaction))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            decimal newBalance = currentBalance + changeAmount;
            decimal minBalance = changeAmount < 0 ? Math.Abs(changeAmount) : 0;
            string updateSql = @"
                UPDATE Balances
                SET Balance = ?, LastUpdatedAt = NOW()
                WHERE UserId = ? AND Balance >= ?;

                INSERT INTO BalanceHistory
                (UserId, ChangeAmount, BalanceBefore, BalanceAfter, Reason)
                VALUES (?, ?, ?, ?, ?);";

            using (var cmd = new MySqlCommand("PREPARE updateStmt FROM @sql;", connection, transaction))
            {
                cmd.Parameters.AddWithValue("@sql", updateSql);
                await cmd.ExecuteNonQueryAsync();
            }
            using (var cmd = new MySqlCommand(@"
                SET 
                @b = @NewBalance,
                @u = @UserId,
                @min = @MinBalance,
                @c = @ChangeAmount,
                @before = @Before,
                @after = @After,
                @r = @Reason;", connection, transaction))
            {
                cmd.Parameters.AddWithValue("@NewBalance", newBalance);
                cmd.Parameters.AddWithValue("@UserId", userId);
                cmd.Parameters.AddWithValue("@MinBalance", minBalance);
                cmd.Parameters.AddWithValue("@ChangeAmount", changeAmount);
                cmd.Parameters.AddWithValue("@Before", currentBalance);
                cmd.Parameters.AddWithValue("@After", newBalance);
                cmd.Parameters.AddWithValue("@Reason", reason);
                await cmd.ExecuteNonQueryAsync();
            }

            int rowsAffected;
            using (var cmd = new MySqlCommand(@"
                EXECUTE updateStmt 
                USING @b, @u, @min, @u, @c, @before, @after, @r;", connection, transaction))
            {
                rowsAffected = await cmd.ExecuteNonQueryAsync();
            }

            using (var cmd = new MySqlCommand("DEALLOCATE PREPARE updateStmt;", connection, transaction))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            if (rowsAffected < 2)
            {
                await transaction.RollbackAsync();
                return BadRequest(new { message = $"Không đủ số dư: ChangeAmount={changeAmount}, Balance={currentBalance}" });
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
