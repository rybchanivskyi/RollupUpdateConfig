using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Web.SessionState;

namespace RollupUpdateConfig
{
    class Program
    {
        static void Main(string[] args)
        {
            const string url = "https://pochtanova.crm4.dynamics.com";
            const string userName = "victor@pochtanova.onmicrosoft.com";
            const string password = "Dox589991";
            const int TypeConfig = 778280000;
            const int TypeTask = 778280001;
            const int TypeErorr = 778280002;
            Guid currencyId = new Guid("b3a963da-2eda-e611-80f2-fc15b42826a0");

            string connectionStr = $@"
            Url = {url};
            AuthType = Office365;
            UserName = {userName};
            Password = {password};
            RequireNewInstance = True";

            try
            {
                using (CrmServiceClient svc = new CrmServiceClient(connectionStr))
                {
                    Console.WriteLine("Try to receive data!");

                    HandleErrors(svc, TypeErorr, currencyId);

                    HandleTasks(svc, TypeTask, TypeConfig, TypeErorr);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred: \n" + ex.Message);
            }

            Console.WriteLine("Press any key to exit.");
            Console.ReadLine();
        }

        public static void HandleErrors(CrmServiceClient svc, int TypeError, Guid currencyId)
        {
            List<Entity> errors = getQuery(svc,
                new[] {"np_rollupupdateconfigid", "np_entitywithrollups", "np_recordid"}, TypeError);


            foreach (var error in errors)
            {
                try
                {
                    if (UpdateErrorRecord(svc, error.Attributes["np_entitywithrollups"].ToString(),
                        new Guid(error.Attributes["np_recordid"].ToString()), currencyId))
                    {
                        ChangeState(svc, "np_rollupupdateconfig", error.Id);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        public static void HandleTasks(CrmServiceClient svc, int TypeTask, int TypeConfig, int TypeError)
        {
            List<Entity> tasks = getQuery(svc,
                new[] { "np_rollupupdateconfigid", "np_entitywithrollups"}, TypeTask);

            foreach (var task in tasks)
            {
                HandleConfigs(svc, task.Attributes["np_entitywithrollups"].ToString(), TypeConfig, TypeError);
                ChangeState(svc, "np_rollupupdateconfig", task.Id);
            }
        }

        public static void HandleConfigs(CrmServiceClient svc, string entityName, int TypeConfig, int TypeError)
        {
            List<Entity> configs = getQuery(svc,
                new[] { "np_rollupupdateconfigid", "np_fields_for_update" }, TypeConfig, entityName);
            foreach (var config in configs)
            {
                if (!config.Attributes.Contains("np_fields_for_update"))
                {
                    continue;
                }

                try
                {
                    QueryExpression query = new QueryExpression(entityName);
                    var recordsOfEntity = svc.RetrieveMultiple(query).Entities;
                    foreach (var record in recordsOfEntity)
                    {
                        foreach (string fieldName in config.Attributes["np_fields_for_update"].ToString().Split(','))
                        {
                            Console.WriteLine($"Try to update record with ID = {record.Id}");
                            try
                            {
                                CalculateRollupFieldRequest crfr = new CalculateRollupFieldRequest
                                {
                                    Target = new EntityReference(entityName, record.Id),
                                    FieldName = fieldName
                                };
                                CalculateRollupFieldResponse
                                    response = (CalculateRollupFieldResponse) svc.Execute(crfr);
                            }
                            catch (Exception ex)
                            {
                                Entity err = new Entity("np_rollupupdateconfig");
                                err["np_recordtype"] = new OptionSetValue(TypeError);
                                err["np_entitywithrollups"] = entityName;
                                err["np_recordid"] = record.Id.ToString();
                                svc.Create(err);
                                Console.WriteLine($"Can`t update record with ID = {record.Id}.Error was created!");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Erorr ocurred when tried to retrieve data from Entity with name {entityName}");
                    Console.WriteLine(ex.Message);
                }
            }
        }

        public static void ChangeState(CrmServiceClient svc, string entityName, Guid Id, int stateCode = 1,
            int statusCode = 2)
        {
            SetStateRequest state = new SetStateRequest
            {
                State = new OptionSetValue(stateCode),
                Status = new OptionSetValue(statusCode),
                EntityMoniker = new EntityReference(entityName, Id)
            };
            svc.Execute(state);
        }

        public static bool UpdateErrorRecord(CrmServiceClient svc, string entityName, Guid id, Guid currencyId)
        {
            try
            {
                svc.Update(new Entity(entityName, id) {["transactioncurrencyid"] = null});
                svc.Update(new Entity(entityName, id) {["transactioncurrencyid"] = currencyId});
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return false;
            }
        }

        public static List<Entity> getQuery(CrmServiceClient svc, string[] columns, int Type, string entityName = "")
        {
            QueryExpression query = new QueryExpression()
            {
                Distinct = false,
                EntityName = "np_rollupupdateconfig",
                ColumnSet = new ColumnSet(columns),
                Criteria =
                {
                    Filters =
                    {
                        new FilterExpression
                        {
                            FilterOperator = LogicalOperator.And,
                            Conditions =
                            {
                                new ConditionExpression("statecode", ConditionOperator.Equal, 0),
                                new ConditionExpression("np_recordtype", ConditionOperator.Equal, Type),
                                Type != 778280000 ? new ConditionExpression("createdon", ConditionOperator.Today) : new ConditionExpression("np_entitywithrollups", ConditionOperator.Equal, entityName)
                            }
                        }
                    }
                }
            };
            int pageNumber = 1;
            query.PageInfo = new PagingInfo();
            query.PageInfo.PageNumber = pageNumber;
            query.PageInfo.PagingCookie = null;
            List<Entity> result = new List<Entity>();
            while(true)
            {
                EntityCollection pageRecords = svc.RetrieveMultiple(query);
                if (pageRecords.Entities != null)
                {
                    result.AddRange(pageRecords.Entities.ToList());
                }

                if (pageRecords.MoreRecords)
                {
                    query.PageInfo.PageNumber++;
                    query.PageInfo.PagingCookie = pageRecords.PagingCookie;
                }
                else
                {
                    break;
                }
            }
            return result;
        }
    }
}
