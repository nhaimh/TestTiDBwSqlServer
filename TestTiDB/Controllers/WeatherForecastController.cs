using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using System.Data;
using System.Diagnostics;

[ApiController]
[Route("api/[controller]")]
public class IRISController : ControllerBase
{
    private readonly string tidbConnStr = "Server=127.0.0.1;Port=4000;User ID=root;Password=;Database=test;";
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
                            // Không đủ điều kiện (vd: rút tiền nhưng số dư không đủ)
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

            //using (var connection = new SqlConnection(sqlServerConnStr))
            //{
            //    await connection.OpenAsync();

            //    decimal currentBalance;
            //    using (var checkCmd = new SqlCommand("SELECT Balance FROM Balances WHERE UserId = @UserId", connection))
            //    {
            //        checkCmd.Parameters.AddWithValue("@UserId", userId);
            //        var result = await checkCmd.ExecuteScalarAsync();
            //        if (result == null)
            //        {
            //            return NotFound(new { message = $"Không tìm thấy UserId={userId} trong Balances." });
            //        }
            //        currentBalance = (decimal)result;
            //    }

            //    if (Math.Abs(changeAmount) > currentBalance)
            //    {
            //        return BadRequest(new { message = $"Rút tiền vượt quá số dư hiện tại: ChangeAmount={changeAmount}, Balance={currentBalance}" });
            //    }
            //    decimal newBalance = currentBalance - changeAmount;

            //    using (var command = new SqlCommand("UpdateUserBalance", connection))
            //    {
            //        command.CommandType = CommandType.StoredProcedure;
            //        command.Parameters.AddWithValue("@UserId", userId);
            //        command.Parameters.AddWithValue("@ChangeAmount", changeAmount); 
            //        command.Parameters.AddWithValue("@Reason", reason);

            //        await command.ExecuteNonQueryAsync();
            //    }

            //    return Ok(new { message = $"[SQL Server] Updated balance: UserId={userId}, ChangeAmount={changeAmount}, BalanceBefore={currentBalance}, BalanceAfter={newBalance}, Reason='{reason}'" });
            //}
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

                        using (var combinedCmd = new SqlCommand(
                            @"UPDATE Balances
                              SET Balance = @NewBalance,
                                  LastUpdatedAt = GETDATE()
                              WHERE UserId = @UserId
                                AND Balance >= @MinBalanceRequired;

                              INSERT INTO BalanceHistory
                                (UserId, ChangeAmount, BalanceBefore, BalanceAfter, Reason, CreatedAt)
                              VALUES
                                (@UserId, @ChangeAmount, @BalanceBefore, @BalanceAfter, @Reason, GETDATE());", connection, (SqlTransaction)transaction))
                        {
                            combinedCmd.Parameters.AddWithValue("@NewBalance", newBalance);
                            combinedCmd.Parameters.AddWithValue("@UserId", userId);
                            combinedCmd.Parameters.AddWithValue("@ChangeAmount", changeAmount);
                            combinedCmd.Parameters.AddWithValue("@BalanceBefore", currentBalance);
                            combinedCmd.Parameters.AddWithValue("@BalanceAfter", newBalance);

                            decimal minBalanceRequired = changeAmount < 0 ? Math.Abs(changeAmount) : 0;
                            combinedCmd.Parameters.AddWithValue("@MinBalanceRequired", minBalanceRequired);
                            combinedCmd.Parameters.AddWithValue("@Reason", reason ?? string.Empty);

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
}
