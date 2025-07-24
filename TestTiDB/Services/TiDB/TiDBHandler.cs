using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using System.Data;

namespace TestTiDB.Services
{
    public class TiDBHandler : ITiDBHandler
    {
        private readonly string tidbConnStr = "Server=127.0.0.1;Port=4000;User ID=root;Password=;Database=test;Allow User Variables=true";
                
        public async Task<HandlerResult> GetTidb()
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
                                return new HandlerResult
                                {
                                    StatusCode = 404,
                                    Message = $"Không tìm thấy UserId={userId} trong Balances."
                                };
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
                                await transaction.RollbackAsync();;
                                return new HandlerResult
                                {
                                    StatusCode = 400,
                                    Message = $"Không đủ số dư: ChangeAmount={changeAmount}, Balance={currentBalance}"
                                };
                            }

                            await transaction.CommitAsync();
                        }
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync();
                        return new HandlerResult
                        {
                            StatusCode = 500,
                            Message = $"[TiDB] Error in UserId={userId}" + ex.Message,
                        };
                    }
                }

                return new HandlerResult
                {
                    StatusCode = 200,
                    Message = $"[TiDB] Updated balance: UserId={userId}, ChangeAmount={changeAmount}, Reason='{reason}'"
                };
            }
        }

        public async Task<HandlerResult> GetTidbOptimized()
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
                        return new HandlerResult
                        {
                            StatusCode = 404,
                            Message = $"Không tìm thấy UserId={userId} trong Balances."
                        };
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
                        return new HandlerResult
                        {
                            StatusCode = 400,
                            Message = $"Không đủ số dư: ChangeAmount={changeAmount}, Balance={currentBalance}"
                        };
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

                return new HandlerResult
                {
                    StatusCode = 200,
                    Message = $"[TiDB] Updated balance: UserId={userId}, ChangeAmount={changeAmount}, Reason='{reason}'"
                };
            }
            catch (Exception ex)
            {
                return new HandlerResult
                {
                    StatusCode = 500,
                    Message = $"[TiDB] Error in UserId={userId}" + ex.Message,
                };
            }
        }

        public async Task<HandlerResult> GetTidbps()
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
                        return new HandlerResult
                        {
                            StatusCode = 404,
                            Message = $"Không tìm thấy UserId={userId} trong Balances."
                        };
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
                    return new HandlerResult
                    {
                        StatusCode = 400,
                        Message = $"Không đủ số dư: ChangeAmount={changeAmount}, Balance={currentBalance}"
                    };
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

                return new HandlerResult
                {
                    StatusCode = 200,
                    Message = $"[TiDB] Updated balance: UserId={userId}, ChangeAmount={changeAmount}, Reason='{reason}'"
                };
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                return new HandlerResult
                {
                    StatusCode = 500,
                    Message = $"[TiDB] Error in UserId={userId}" + ex.Message,
                };
            }
        }
    }
    public class HandlerResult
    {
        public int StatusCode { get; set; }
        public object? Data { get; set; }
        public string? Message { get; set; }
    }
}
