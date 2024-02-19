using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;
namespace WordService;

public class Coordinator
{
    private IDictionary<string, DbConnection> ConnectionCache = new Dictionary<string, DbConnection>();
    private const string DOCUMENT_DB = "document-db";
    private const string OCCURRENCE_DB = "occurrence-db";
    private const string SHORT_WORD_DB = "short-word-db";
    private const string MEDIUM_WORD_DB = "medium-word-db";
    private const string LONG_WORD_DB = "long-word-db";

    public async Task<DbConnection> GetDocumentConnectionAsync()
    {
        return await GetConnectionByServerNameAsync(DOCUMENT_DB);
    }

    public async Task<DbConnection> GetOccurrenceConnectionAsync()
    {
        return await GetConnectionByServerNameAsync(OCCURRENCE_DB);
    }

    public async Task<DbConnection> GetWordConnectionAsync(string word)
    {
        switch (word.Length)
        {
            case var l when (l <= 10):
                return await GetConnectionByServerNameAsync(SHORT_WORD_DB);
            case var l when (l > 10 && l <= 20):
                return await GetConnectionByServerNameAsync(MEDIUM_WORD_DB);
            case var l when (l >= 21):
                return await GetConnectionByServerNameAsync(LONG_WORD_DB);
            default:
                throw new InvalidDataException();
        }
    }

    public async IAsyncEnumerable<DbConnection> GetAllConnectionsAsync()
    {
        yield return await GetDocumentConnectionAsync();
        yield return await GetOccurrenceConnectionAsync();
        await foreach (var wordConnection in GetAllWordConnectionsAsync())
        {
            yield return wordConnection;
        }
    }

    public async IAsyncEnumerable<DbConnection> GetAllWordConnectionsAsync()
    {
        yield return await GetConnectionByServerNameAsync(SHORT_WORD_DB);
        yield return await GetConnectionByServerNameAsync(MEDIUM_WORD_DB);
        yield return await GetConnectionByServerNameAsync(LONG_WORD_DB);
    }

    private async Task<DbConnection> GetConnectionByServerNameAsync(string serverName)
    {
        if (ConnectionCache.TryGetValue(serverName, out var connection))
        {
            return connection;
        }

        connection = new SqlConnection($"Server={serverName};User Id=sa;Password=SuperSecret7!;Encrypt=false;");

        await connection.OpenAsync();

        ConnectionCache.Add(serverName, connection);
        return connection;
    }
}
