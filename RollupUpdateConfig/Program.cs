using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
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

            string connectionStr = $@"
            Url = {url};
            AuthType = Office365;
            UserName = {userName};
            Password = {password};
            RequireNewInstance = True";

            try
            {
                using (var svc = new CrmServiceClient(connectionStr))
                {
                    Console.WriteLine("Try to receive data!");

                    var errors = (from rollupUpdateConfig in new OrganizationServiceContext(svc).CreateQuery("np_rollupupdateconfig")
                        where ((OptionSetValue)rollupUpdateConfig["statecode"]).Value == 0 
                              && ((OptionSetValue)rollupUpdateConfig["np_recordtype"]).Value == TypeErorr 
                              && (DateTime)rollupUpdateConfig["createdon"] >= DateTime.Today
                                  select new
                        {
                            Id = (Guid)rollupUpdateConfig["np_rollupupdateconfigid"],
                            Entity = rollupUpdateConfig.Contains("np_entitywithrollups") ? rollupUpdateConfig["np_entitywithrollups"].ToString() : "",
                            RecordId = rollupUpdateConfig.Contains("np_recordid") ? rollupUpdateConfig["np_recordid"].ToString() : ""
                        }).ToList();
                    foreach (var error in errors)
                    {
                        //update error field

                        //error record set "statecode" = 1
                        SetStateRequest state = new SetStateRequest();
                        state.State = new OptionSetValue(1);
                        state.Status = new OptionSetValue(2);
                        state.EntityMoniker = new EntityReference("np_rollupupdateconfig", error.Id);
                        svc.Execute(state);
                    }

                    var tasks = (from rollupUpdateConfig in new OrganizationServiceContext(svc).CreateQuery("np_rollupupdateconfig")
                        where ((OptionSetValue)rollupUpdateConfig["statecode"]).Value == 0 
                              && ((OptionSetValue)rollupUpdateConfig["np_recordtype"]).Value == TypeTask
                              && (DateTime)rollupUpdateConfig["createdon"] >= DateTime.Today
                        select new
                        {
                            Id = (Guid)rollupUpdateConfig["np_rollupupdateconfigid"],
                            Entity = rollupUpdateConfig.Contains("np_entitywithrollups") ? rollupUpdateConfig["np_entitywithrollups"].ToString() : ""
                        }).ToList();

                    foreach (var task in tasks)
                    {
                        string entityName = task.Entity;
                        var configs = (from rollupUpdateConfig in new OrganizationServiceContext(svc).CreateQuery("np_rollupupdateconfig")
                            where ((OptionSetValue)rollupUpdateConfig["statecode"]).Value == 0 
                                  && ((OptionSetValue)rollupUpdateConfig["np_recordtype"]).Value == TypeConfig
                                  && (string)rollupUpdateConfig["np_entitywithrollups"] == entityName
                            select new
                            {
                                Id = (Guid)rollupUpdateConfig["np_rollupupdateconfigid"],
                                Entity = entityName,
                                FieldsForUpdate = rollupUpdateConfig.Contains("np_fields_for_update") ? rollupUpdateConfig["np_fields_for_update"].ToString() : ""
                            }).ToList();
                        //UPDATE FIELDS OF CONFIG
                        foreach (var config in configs)
                        {
                            try
                            {
                                //try to update all fields from config in config.entity
                                CalculateRollupFieldRequest crfr = new CalculateRollupFieldRequest
                                {
                                    Target = new EntityReference(config.Entity),
                                    FieldName = "" //name of config.FieldsForUpdate
                                };
                                CalculateRollupFieldResponse response = (CalculateRollupFieldResponse)svc.Execute(crfr);
                            }
                            catch (Exception ex)
                            {
                                //create new record of type 
                            }
                        }
                        SetStateRequest state = new SetStateRequest();
                        state.State = new OptionSetValue(1);
                        state.Status = new OptionSetValue(2);
                        state.EntityMoniker = new EntityReference("np_rollupupdateconfig", task.Id);
                        svc.Execute(state);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred: \n" + ex.Message);
            }
            Console.WriteLine("Press any key to exit.");
            Console.ReadLine();
        }
    }
}
