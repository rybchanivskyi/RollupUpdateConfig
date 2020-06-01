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
using System.Text;
using System.Threading.Tasks;

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

                    HandleErrors(svc, TypeErorr, TypeConfig, currencyId);

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

        public static void HandleErrors(CrmServiceClient svc, int TypeErorr, int TypeConfig, Guid currencyId)
        {
            var errors =
                (from rollupUpdateConfig in new OrganizationServiceContext(svc).CreateQuery("np_rollupupdateconfig")
                    where ((OptionSetValue) rollupUpdateConfig["statecode"]).Value == 0
                          && ((OptionSetValue) rollupUpdateConfig["np_recordtype"]).Value == TypeErorr
                          && (DateTime) rollupUpdateConfig["createdon"] >= DateTime.Today
                    select new
                    {
                        Id = (Guid) rollupUpdateConfig["np_rollupupdateconfigid"],
                        Entity = rollupUpdateConfig.Contains("np_entitywithrollups")
                            ? rollupUpdateConfig["np_entitywithrollups"].ToString()
                            : "",
                        RecordId = rollupUpdateConfig.Contains("np_recordid")
                            ? new Guid(rollupUpdateConfig["np_recordid"].ToString())
                            : Guid.Empty
                    }).ToList();
            foreach (var error in errors)
            {
                if (UpdateErrorRecord(svc, error.Entity, error.RecordId, TypeConfig, currencyId))
                {
                    ChangeState(svc, "np_rollupupdateconfig", error.Id);
                }
            }
        }

        public static void HandleTasks(CrmServiceClient svc, int TypeTask, int TypeConfig, int TypeError)
        {
            var tasks =
                (from rollupUpdateConfig in new OrganizationServiceContext(svc).CreateQuery("np_rollupupdateconfig")
                    where ((OptionSetValue) rollupUpdateConfig["statecode"]).Value == 0
                          && ((OptionSetValue) rollupUpdateConfig["np_recordtype"]).Value == TypeTask
                          && (DateTime) rollupUpdateConfig["createdon"] >= DateTime.Today
                    select new
                    {
                        Id = (Guid) rollupUpdateConfig["np_rollupupdateconfigid"],
                        Entity = rollupUpdateConfig.Contains("np_entitywithrollups")
                            ? rollupUpdateConfig["np_entitywithrollups"].ToString()
                            : ""
                    }).ToList();

            foreach (var task in tasks)
            {
                HandleConfigs(svc, task.Entity, TypeConfig, TypeError);
                ChangeState(svc, "np_rollupupdateconfig", task.Id);
            }
        }

        public static void HandleConfigs(CrmServiceClient svc, string entityName, int TypeConfig, int TypeError)
        {
            var configs =
                (from rollupUpdateConfig in new OrganizationServiceContext(svc).CreateQuery("np_rollupupdateconfig")
                    where ((OptionSetValue) rollupUpdateConfig["statecode"]).Value == 0
                          && ((OptionSetValue) rollupUpdateConfig["np_recordtype"]).Value == TypeConfig
                          && (string) rollupUpdateConfig["np_entitywithrollups"] == entityName
                    select new
                    {
                        Id = (Guid) rollupUpdateConfig["np_rollupupdateconfigid"],
                        Entity = entityName,
                        FieldsForUpdate = rollupUpdateConfig.Contains("np_fields_for_update")
                            ? rollupUpdateConfig["np_fields_for_update"].ToString()
                            : ""
                    }).ToList();
            foreach (var config in configs)
            {
                var recordsOfEntity = (from entity in new OrganizationServiceContext(svc).CreateQuery(config.Entity)
                    select new
                    {
                        Id = (Guid) entity[config.Entity + "id"]
                    }).ToList();
                foreach (var record in recordsOfEntity)
                {
                    foreach (string fieldName in config.FieldsForUpdate.Split(','))
                    {
                        Console.WriteLine($"Try to update record with ID = {record.Id}");
                        try
                        {
                            CalculateRollupFieldRequest crfr = new CalculateRollupFieldRequest
                            {
                                Target = new EntityReference(config.Entity, record.Id),
                                FieldName = fieldName
                            };
                            CalculateRollupFieldResponse response = (CalculateRollupFieldResponse) svc.Execute(crfr);
                        }
                        catch (Exception ex)
                        {
                            Entity err = new Entity("np_rollupupdateconfig");
                            err["np_recordtype"] = new OptionSetValue(TypeError);
                            err["np_entitywithrollups"] = config.Entity;
                            err["np_recordid"] = record.Id;
                            svc.Create(err);
                            Console.WriteLine($"Can`t update record with ID = {record.Id}.Error was created!");
                        }
                    }
                }
            }
        }

        public static void ChangeState(CrmServiceClient svc, string entityName, Guid Id, int stateCode = 1,
            int statusCode = 2)
        {
            SetStateRequest state = new SetStateRequest();
            state.State = new OptionSetValue(stateCode);
            state.Status = new OptionSetValue(statusCode);
            state.EntityMoniker = new EntityReference(entityName, Id);
            svc.Execute(state);
        }

        public static bool UpdateErrorRecord(CrmServiceClient svc, string entityName, Guid Id, int TypeConfig, Guid currencyId)
        {
            var configs =
                (from rollupUpdateConfig in new OrganizationServiceContext(svc).CreateQuery("np_rollupupdateconfig")
                    where ((OptionSetValue) rollupUpdateConfig["statecode"]).Value == 0
                          && ((OptionSetValue) rollupUpdateConfig["np_recordtype"]).Value == TypeConfig
                          && (string) rollupUpdateConfig["np_entitywithrollups"] == entityName
                    select new
                    {
                        Id = (Guid) rollupUpdateConfig["np_rollupupdateconfigid"],
                        Entity = entityName,
                        FieldsForUpdate = rollupUpdateConfig.Contains("np_fields_for_update")
                            ? rollupUpdateConfig["np_fields_for_update"].ToString()
                            : ""
                    }).ToList();
            for (int i = 0; i < 2; ++i)
            {
                foreach (var config in configs)
                {
                    foreach (string fieldName in config.FieldsForUpdate.Split(','))
                    {
                        try
                        {
                            //change currency
                            ColumnSet attributes = new ColumnSet("transactioncurrencyid");
                            Entity record = svc.Retrieve(entityName, Id, attributes);
                            record["transactioncurrencyid"] = i == 0 ? Guid.Empty : currencyId;
                            CalculateRollupFieldRequest crfr = new CalculateRollupFieldRequest
                            {
                                Target = new EntityReference(entityName, Id),
                                FieldName = fieldName
                            };
                            CalculateRollupFieldResponse response = (CalculateRollupFieldResponse) svc.Execute(crfr);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                            return false;
                        }
                    }
                }
            }

            return true;
        }
    }
}
