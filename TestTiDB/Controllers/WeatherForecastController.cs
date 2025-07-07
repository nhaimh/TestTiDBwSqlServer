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
                    decimal newBalance;
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
                            await transaction.RollbackAsync();
                            Console.WriteLine($"Không tìm thấy UserId={userId}");
                            return NotFound(new { message = $"Không tìm thấy UserId={userId} trong Balances." });
                        }

                        currentBalance = (decimal)result;
                        decimal minBalanceRequired = changeAmount < 0 ? Math.Abs(changeAmount) : 0;
                        if (currentBalance < minBalanceRequired)
                        {
                            await transaction.RollbackAsync();
                            Console.WriteLine($"Rollback for UserId={userId};ChangeAmount={changeAmount}");
                            return BadRequest(new { message = $"Không đủ số dư: ChangeAmount={changeAmount}, Balance={currentBalance}" });
                        }

                        newBalance = currentBalance + changeAmount;
                    }

                    int rowsAffected;
                    using (var updateCmd = new MySqlCommand(
                        @"UPDATE Balances
                          SET Balance = @NewBalance,
                              LastUpdatedAt = NOW()
                          WHERE UserId = @UserId;", connection, transaction))
                    {
                        updateCmd.Parameters.AddWithValue("@NewBalance", newBalance);
                        updateCmd.Parameters.AddWithValue("@UserId", userId);

                        rowsAffected = await updateCmd.ExecuteNonQueryAsync();
                    }

                    if (rowsAffected != 1)
                    {
                        await transaction.RollbackAsync();
                        Console.WriteLine($"Rollback for UserId={userId}");
                        return StatusCode(500, new { message = "Cập nhật số dư thất bại. Không thực hiện ghi log." });
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
                    return Ok(new { message = "Cập nhật thành công", BalanceAfter = newBalance });
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync();
                    Console.WriteLine($"Rollback for UserId={userId}, Reason: {ex.Message}");
                    return StatusCode(500, new { message = "Lỗi server", detail = ex.Message });
                }

            }
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
