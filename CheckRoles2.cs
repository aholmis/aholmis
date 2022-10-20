// See https://aka.ms/new-console-template for more information

using System.Collections.Concurrent;
using Dapper;
using System.Diagnostics;
using System.Text.Json;

Console.WriteLine("Hello, insert run type on of (pre, post, compare)");

string runType = Console.ReadLine() ?? throw new Exception("Need a run type");
Console.WriteLine($"Running {runType}");

var cToken = new CancellationTokenSource().Token;

Stopwatch sw = Stopwatch.StartNew();
var extractionTypes = new[] { "pre", "post" };
if (extractionTypes.Contains(runType))
{
    await Runner.SaveRolesToFile(runType, cToken);
}
else if (runType == "compare")
{
    await Runner.Compare(cToken);
}

sw.Stop();

Console.WriteLine($"Total time: {sw.Elapsed}");

public static class Runner
{
    private const string DirCheckRoles = @"C:\Temp\CheckRoles";
    private const string ConnectionString = @"";

    static Runner()
    {
        if (!Directory.Exists(DirCheckRoles))
        {
            Directory.CreateDirectory(DirCheckRoles);
        }
    }

    public static async Task SaveRolesToFile(string runType, CancellationToken cToken)
    {
        var roles = await GetRoles();
        await SaveToFile(roles, runType, cToken);
    }

    private static async Task<IEnumerable<Row>> GetRoles()
    {
        await using System.Data.SqlClient.SqlConnection connection = new(ConnectionString);

        const string sql = @"

        SELECT
            top(10000)
            u.mDomainAlias as 'Domain',
            u.mSignature as 'Signature',

            STUFF(
            (
                SELECT
                    DISTINCT
                    ',' + r.mAlias
                FROM
                    Role r WITH (NOLOCK)
                    JOIN Role_forGroup r_g WITH (NOLOCK) ON r_g.rHasRole = r.C_OID
                    JOIN Group_hasUser g_u WITH (NOLOCK) ON g_u.rMemberOf = r_g.rForGroup
                WHERE
                    g_u.rHasUser = u.C_OID
                FOR XML PATH('')
            ), 1, 1, '') AS 'Services',

            STUFF(
            (
                SELECT
                    DISTINCT
                    ',' + s.mAlias
                FROM
                    Role r WITH (NOLOCK)
                    JOIN Role_forGroup r_g WITH (NOLOCK) ON r_g.rHasRole = r.C_OID
                    JOIN Group_hasUser g_u WITH (NOLOCK) ON g_u.rMemberOf = r_g.rForGroup
                    JOIN Service_hasRole s_r WITH (NOLOCK) ON s_r.rHasRole = r.C_OID
                    JOIN Service s WITH (NOLOCK) ON s_r.rHasService = s.C_OID
                WHERE
                    g_u.rHasUser = u.C_OID
                FOR XML PATH('')
            ), 1, 1, '') AS 'Roles'

        FROM
	        User u WITH (NOLOCK)
        ORDER BY 'Domain' ASC, 'Signature' ASC, 'Services' ASC, 'Roles' ASC

";
        IEnumerable<Row>? rolesForUser = await connection.QueryAsync<Row>(sql);
        return rolesForUser ?? Enumerable.Empty<Row>();
    }

    private static async Task SaveToFile(IEnumerable<Row> roles, string runType, CancellationToken cToken)
    {
        string fileName = @$"{DirCheckRoles}\Result_{runType}.csv";

        string content = string.Join('\n', roles);
        await File.WriteAllTextAsync(fileName, content, cToken);
    }

    private static async Task<IEnumerable<string>> GetDeletedServices()
    {
        await using System.Data.SqlClient.SqlConnection connection = new(ConnectionString);

        const string sql = @"
select distinct mAlias from [cleanup].[Role] 
";
        var deleted = await connection.QueryAsync<string>(sql);
        return deleted ?? Enumerable.Empty<string>();
    }

    private static async Task<IEnumerable<string>> GetDeletedRoles()
    {
        await using System.Data.SqlClient.SqlConnection connection = new(ConnectionString);

        const string sql = @"
select distinct mAlias from [cleanup].[Service] 
";
        var deleted = await connection.QueryAsync<string>(sql);
        return deleted ?? Enumerable.Empty<string>();
    }

    public static async Task Compare(CancellationToken cToken)
    {
        string preFileName = @$"{DirCheckRoles}\Result_pre.csv";
        string postFileName = @$"{DirCheckRoles}\Result_post.csv";
        string diffFileName = @$"{DirCheckRoles}\Result_diff.csv";

        string[] preAllLines = await File.ReadAllLinesAsync(preFileName, cToken);
        string[] postAllLines = await File.ReadAllLinesAsync(postFileName, cToken);

        if (preAllLines.Length != postAllLines.Length)
        {
            Console.WriteLine($"Pre lines  {preAllLines.Length}");
            Console.WriteLine($"does not match");
            Console.WriteLine($"Post lines {postAllLines.Length}");
        }

        ConcurrentDictionary<int, CompareRow> preRows = new();

        Parallel.ForEach(preAllLines, line =>
        {
            var elements = line.Split(';');
            var row = new CompareRow(elements[0], elements[1], elements[2]);
            preRows.TryAdd(row.GetHashCode(), row);
        });

        ConcurrentDictionary<int, CompareRow> postRows = new();

        Parallel.ForEach(postAllLines, line =>
        {
            var elements = line.Split(';');
            var row = new CompareRow(elements[0], elements[1], elements[2]);
            postRows.TryAdd(row.GetHashCode(), row);
        });

        var deletedServices = new HashSet<string>(await GetDeletedServices());
        var deletedRoles = new HashSet<string>(await GetDeletedRoles());
        var diffRows = new List<CompareRow>();

        foreach (KeyValuePair<int, CompareRow> preRow in preRows)
        {
            bool exist = postRows.TryGetValue(preRow.Key, out var postRow);
            var serviceDiff = exist ? CompareContent(preRow.Value.Services, postRow.Services, deletedServices) : "Gone";
            var rolesDiff = exist ? CompareContent(preRow.Value.Roles, postRow.Roles, deletedRoles) : "Gone";
            if (string.IsNullOrWhiteSpace(serviceDiff) || string.IsNullOrWhiteSpace(rolesDiff))
            {
                continue;
            }

            CompareRow diff = new(preRow.Value.DomainAndSignature, serviceDiff, rolesDiff);
            diffRows.Add(diff);
        }

        await File.WriteAllLinesAsync(diffFileName, diffRows.Select(r => r.ToString()), cToken);
    }

    private static string CompareContent(string preRow, string postRow, HashSet<string> deletedContent)
    {
        if (preRow == postRow)
        {
            return "";
        }

        var preServices = new HashSet<string>(preRow.Split(','));
        var postServices = new HashSet<string>(postRow.Split(','));
        preServices.SymmetricExceptWith(postServices);
        preServices.ExceptWith(deletedContent);
        return string.Join(',', preServices);
    }

}

public record struct Row(string Domain, string Signature, string Services, string Roles)
{
    public override string ToString()
    {
        return $"{Domain}-{Signature};{Services};{Roles}";
    }
}

public record struct CompareRow(string DomainAndSignature, string Services, string Roles)
{
    public override int GetHashCode() => DomainAndSignature.GetHashCode();
    public override string ToString()
    {
        return $"{DomainAndSignature};{Services};{Roles}";
    }
}

public record struct User(string Domain, string Signature)
{
    public override string ToString()
    {
        return $"{Domain}-{Signature}";
    }
}

public record struct Diff(CompareRow Row)
{
    public override string ToString()
    {
        return $"{Row}";
    }
}
