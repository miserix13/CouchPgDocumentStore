using Npgsql;
using NpgsqlTypes;
using System.Text.Json;

namespace CouchPgDocumentStore
{
    public class DocumentManager : IAsyncDisposable
    {
        private readonly NpgsqlDataSource dataSource;

        public async ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            await this.dataSource.DisposeAsync();
        }

        public DocumentManager(NpgsqlDataSource source) :
            base()
        {
            this.dataSource = source;
        }

        private async Task<int> createCollectionTableAsync(string collectionName)
        {
            int j = 0;

            await using var cmd = this.dataSource.CreateCommand($"CREATE TABLE IF NOT EXISTS {collectionName} (doc_data JSONB NOT NULL);");
            j += await cmd.ExecuteNonQueryAsync();

            return j;
        }

        public async Task<int> InsertAsync<TEntity>(string collectionName, IEnumerable<TEntity> entities)
        {
            int j = 0;

            j += await this.createCollectionTableAsync(collectionName);
            await using var cmd = this.dataSource.CreateCommand($"INSERT INTO {collectionName} VALUES ($1);");

            foreach (var item in entities)
            {
                string json = JsonSerializer.Serialize<TEntity>(item);
                cmd.Parameters.AddWithValue(NpgsqlDbType.Jsonb, json);
                j += await cmd.ExecuteNonQueryAsync();
                cmd.Parameters.Clear();
            }

            return j;
        }

        public async IAsyncEnumerable<TEntity> GetDocumentsAsync<TEntity>(string collectionName)
        {
            await using var cmd = this.dataSource.CreateCommand($"SELECT doc_data FROM {collectionName};");
            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                TEntity entity = JsonSerializer.Deserialize<TEntity>(reader.GetString(0));
                yield return entity;
            }
        }
    }
}
