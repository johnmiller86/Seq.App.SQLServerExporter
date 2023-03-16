using Newtonsoft.Json;
using Seq.Apps.LogEvents;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Seq.Apps.SQLServerExporter
{
    [SeqApp(Constants.AppMetadata.AppName, Description = Constants.AppMetadata.AppDescription)]
    public class SQLServerExporter : SeqApp, ISubscribeTo<LogEventData>
    {
        #region App Settings
        [SeqAppSetting(DisplayName = Constants.AppFields.ConnectionStringDisplayName, HelpText = Constants.AppFields.ConnectionStringHelpText, InputType = SettingInputType.Password, IsOptional = false)]
        public string ConnectionString { get; set; }
        [SeqAppSetting(DisplayName = Constants.AppFields.SchemaNameDisplayName, HelpText = Constants.AppFields.SchemaNameHelpText, InputType = SettingInputType.Text, IsOptional = true)]
        public string SchemaName { get; set; }
        [SeqAppSetting(DisplayName = Constants.AppFields.TableNameDisplayName, HelpText = Constants.AppFields.TableNameHelpText, InputType = SettingInputType.Text, IsOptional = true)]
        public string TableName { get; set; }
        #endregion

        #region Overrides/Interface Implementations
        protected override void OnAttached()
        {
            ValidateConnection();
            PerformInitialSetup();
        }

        public void On(Event<LogEventData> evt)
        {
            // Process the event
            ProcessEvent(evt);
        }
        #endregion

        #region Private Methods
        private void ValidateConnection()
        {
            try
            {
                // Initializing Connection
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    // Opening Connection
                    conn.Open();
                }
            }
            catch (Exception ex)
            {
                Log.ForContext(Constants.LogContextConstants.ConnectionString, ConnectionString)
                    .Error(string.Format(Constants.LogMessageConstants.ErrorFormat, Constants.LogMessageConstants.ConnectionError, ex.ToString()));
                throw new SeqAppException(Constants.SeqAppExceptions.ConnectionException);
            }
        }

        private void PerformInitialSetup()
        {
            SetDefaults();
            CreateSchema();
            CreateTable();
        }

        private void SetDefaults()
        {
            SchemaName = !string.IsNullOrEmpty(SchemaName) ? SchemaName : Constants.DatabaseConstants.DefaultSchemaName;
            TableName = !string.IsNullOrEmpty(TableName) ? TableName : Constants.DatabaseConstants.DefaultTableName;
        }

        private void CreateSchema()
        {
            // Not Necessary to Create Default dbo Schema
            if (!SchemaName.Equals(Constants.DatabaseConstants.DefaultSchemaName))
            {
                try
                {
                    // Initializing Connection
                    using (var conn = new SqlConnection(ConnectionString))
                    {
                        // Opening Connection
                        conn.Open();

                        // Initializing Command
                        using (var cmd = new SqlCommand(Scripts.CreateSchema, conn))
                        {
                            // Adding Parameters
                            cmd.Parameters.AddWithValue(Constants.SqlParameters.SchemaNameParameter, SchemaName);

                            // Executing Query
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                catch (Exception ex)
                {
                    Log.ForContext(Constants.LogContextConstants.SchemaName, SchemaName)
                    .Error(string.Format(Constants.LogMessageConstants.ErrorFormat, Constants.LogMessageConstants.SchemaCreationError, ex.ToString()));
                    throw new SeqAppException(Constants.SeqAppExceptions.SchemaCreationException);
                }
            }
        }

        private void CreateTable()
        {
            try
            {
                // Initializing Connection
                using (var conn = new SqlConnection(ConnectionString))
                {
                    // Opening Connection
                    conn.Open();

                    // Initializing Command
                    using (var cmd = new SqlCommand(Scripts.CreateTable, conn))
                    {
                        // Adding Parameters
                        cmd.Parameters.AddWithValue(Constants.SqlParameters.SchemaNameParameter, SchemaName);
                        cmd.Parameters.AddWithValue(Constants.SqlParameters.TableNameParameter, TableName);

                        // Executing Query
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Log.ForContext(Constants.LogContextConstants.SchemaName, SchemaName)
                    .ForContext(Constants.LogContextConstants.TableName, TableName)
                    .Error(string.Format(Constants.LogMessageConstants.ErrorFormat, Constants.LogMessageConstants.TableCreationError, ex.ToString()));
                throw new SeqAppException(string.Format(Constants.SeqAppExceptions.TableCreationException, !TableName.Equals(Constants.DatabaseConstants.DefaultTableName) ? ", please check Table Name parameter" : string.Empty));
            }
        }

        private void ProcessEvent(Event<LogEventData> evt)
        {
            // Generate the columns and values dictionary
            var colVals = GetEventColumnValueDictionary(evt);

            // Insert the event
            InsertEvent(colVals);
        }

        private void InsertEvent(Dictionary<string, string> colVals)
        {
            try
            {
                // Initializing Connection
                using (var conn = new SqlConnection(ConnectionString))
                {
                    // Opening Connection
                    conn.Open();

                    // Initializing Command
                    using (var cmd = new SqlCommand(Scripts.InsertEvent, conn))
                    {
                        // Adding Parameters
                        cmd.Parameters.AddWithValue(Constants.SqlParameters.SchemaNameParameter, SchemaName);
                        cmd.Parameters.AddWithValue(Constants.SqlParameters.TableNameParameter, TableName);
                        cmd.Parameters.AddWithValue(Constants.SqlParameters.ColumnsParameter, colVals.Keys.Select(col => col.FormatColumnWithBrackets()).JoinEnumerableWithCommas());
                        cmd.Parameters.AddWithValue(Constants.SqlParameters.ValuesParameter, colVals.Values.Select(val => val.FormatColumnValueWithSingleQuotes()).JoinEnumerableWithCommas());

                        // Executing Query
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Log.ForContext(Constants.LogContextConstants.SchemaName, SchemaName)
                       .ForContext(Constants.LogContextConstants.TableName, TableName)
                       .Error(string.Format(Constants.LogMessageConstants.ErrorFormat, Constants.LogMessageConstants.EventInsertionError, ex.ToString()));
                throw new SeqAppException(Constants.SeqAppExceptions.EventInsertionException);
            }
        }

        private Dictionary<string, string> GetEventColumnValueDictionary(Event<LogEventData> evt)
        {
            try
            {
                return new Dictionary<string, string>()
                {
                    { Constants.InitialColumns.SeqEventId, evt.Id },
                    { Constants.InitialColumns.SeqEventIngestionTimestamp, evt.TimestampUtc.ToString() },
                    { Constants.InitialColumns.SeqEventLocalTimestamp, evt.Data.LocalTimestamp.ToString() },
                    { Constants.InitialColumns.SeqEventLevel, evt.Data.Level.ToString() },
                    { Constants.InitialColumns.SeqEventMessage, evt.Data.RenderedMessage },
                    { Constants.InitialColumns.SeqEventPropertiesJSON, JsonConvert.SerializeObject(evt.Data.Properties) }
                };
            }
            catch (Exception ex)
            {
                Log.ForContext(Constants.LogContextConstants.EventId, evt.Id)
                       .Error(string.Format(Constants.LogMessageConstants.ErrorFormat, Constants.LogMessageConstants.InsertStatementGenerationErrror, ex.ToString()));
                throw new SeqAppException(Constants.SeqAppExceptions.InsertStatementGenerationException);
            }
        }
        #endregion
    }
}