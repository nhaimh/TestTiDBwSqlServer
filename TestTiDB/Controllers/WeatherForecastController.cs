using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using System;
using System.Data;
using System.Diagnostics;
using TestTiDB.Services;

[ApiController]
[Route("api/[controller]")]
public class IRISController : ControllerBase
{
    private readonly ITiDBHandler _handler;
    private readonly ISQLServerhandler _SQLServerhandler;
    private readonly ICRHandler _CRHandler;
    public IRISController(ITiDBHandler handler, ISQLServerhandler SQLServerhandler, ICRHandler cRHandler)
    {
        _handler = handler;
        _SQLServerhandler = SQLServerhandler;
        _CRHandler = cRHandler;
    }
    [HttpGet("tidb")]
    public async Task<IActionResult> GetTidb()
    {
        var result = await _handler.GetTidb();
        return StatusCode(result.StatusCode, result.Data ?? new { message = result.Message });
    }

    [HttpGet("tidbnew")]
    public async Task<IActionResult> GetTidbOptimized()
    {
        var result = await _handler.GetTidbOptimized();
        return StatusCode(result.StatusCode, result.Data ?? new { message = result.Message });
    }

    // Sử dụng PS
    [HttpGet("tidbps")]
    public async Task<IActionResult> GetTidbps()
    {
        var result = await _handler.GetTidbps();
        return StatusCode(result.StatusCode, result.Data ?? new { message = result.Message });
    }
    // Sử dụng rawSQL
    [HttpGet("sqlserver")]
    public async Task<IActionResult> GetSqlServer()
    {
        var result = await _SQLServerhandler.GetSqlServer();
        return StatusCode(result.StatusCode, result.Data ?? new { message = result.Message });
    }
    //Sử dụng stored procedure
    [HttpGet("sqlserversp")]
    public async Task<IActionResult> GetSqlServerWithSP()
    {
        var result = await _SQLServerhandler.GetSqlServerWithSP();
        return StatusCode(result.StatusCode, result.Data ?? new { message = result.Message });
    }
    [HttpGet("crdb")]
    public async Task<IActionResult> GetCockroach()
    {
        var result = await _CRHandler.Get();
        return StatusCode(result.StatusCode, result.Data ?? new { message = result.Message });
    }
    //API test tính active-active của CRDB
    [HttpGet("active")]
    public async Task<IActionResult> Active()
    {
        var result = await _CRHandler.Active();
        return StatusCode(result.StatusCode, result.Data ?? new { message = result.Message });
    }

   

}
