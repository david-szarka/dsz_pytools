import clr
clr.AddReference('System')
clr.AddReference('System.Data')
import System
from System.Data import *
from System.Data.SqlClient import *
import pandas as pd
#https://docs.microsoft.com/en-us/dotnet/api/system.data.sqlclient?view=netframework-4.8
#print(System.DateTime.Now)


def con_create(con_string):
    return SqlConnection(con_string)


def con_open(con):
    con.Open()
    return con


def con_close(con):
    con.Close()
    return con

def adapter_select(query, con, command_timeout = None):
    adapter = SqlDataAdapter(query, con)
    if command_timeout:
        adapter.SelectCommand.CommandTimeout = command_timeout
    return adapter


def fill_to_dataset(adapter = None, dataSet = None, result_name = None):
    if not dataSet:
        dataSet = System.Data.DataSet()
    adapter.Fill(dataSet, result_name)
    return dataSet


def dataset_to_table(dataSet, resultname):
    table = dataSet.Tables[resultname]
    return table


def pull_data_adapter(connection, selectquerry, tablename, timeout = None):
    adapter = adapter_select(selectquerry, connection, command_timeout = timeout)
    dataset = fill_to_dataset(adapter, result_name = tablename)
    table = dataset_to_table(dataset, tablename)
    return table


def con_pull_data_adapter(connectionstring, selectquerry, tablename, timeout = None):
    newcon = con_create(connectionstring)
    newcon = con_open(newcon)
    table =  pull_data_adapter(newcon, selectquerry, tablename, timeout)
    newcon = con_close(newcon)
    return table


def pull_data_reader(connection, selectquerry):
    cmd = SqlCommand(selectquerry,connection)
    rdr = cmd.ExecuteReader()
    result_table = reader_to_datatable(rdr)
    rdr.Close()
    return result_table


def con_pull_data_reader(connectionstring, selectquerry):
    newcon = con_create(connectionstring)
    newcon = con_open(newcon)
    result_table = pull_data_reader(newcon, selectquerry)
    newcon = con_close(newcon)
    return result_table


def reader_to_datatable(reader):
    data = DataTable()
    for coli in range(reader.FieldCount):
        #sqldt = reader.GetDataTypeName(coli)
        data.Columns.Add(reader.GetName(coli), reader.GetFieldType(coli))
    if reader.HasRows:
        while reader.Read():
            newRow = data.NewRow()
            dc = reader.FieldCount
            data.Rows.Add([reader[i] for i in  range(reader.FieldCount)])
    return data


def query_data_reader(connection, query, timeout = None):
    cmd = connection.CreateCommand()
    cmd.CommandType = CommandType.Text
    if timeout:
        cmd.CommandTimeout = timeout
    cmd.CommandText = query
    rdr = cmd.ExecuteReader()
    result_table = reader_to_datatable(rdr)
    rdr.Close()
    return result_table


def con_query_data_reader(connectionstring, query):
    newcon = con_create(connectionstring)
    newcon = con_open(newcon)
    result_table = query_data_reader(newcon, query)
    newcon = con_close(newcon)
    return result_table


def sp_data_reader(connection, stored_procedure, timeout = None):
    cmd = connection.CreateCommand()
    cmd.CommandType = CommandType.StoredProcedure
    if timeout:
        cmd.CommandTimeout = timeout
    cmd.CommandText = stored_procedure
    rdr = cmd.ExecuteReader()
    result_table = reader_to_datatable(rdr)
    rdr.Close()
    return result_table


def con_sp_data_reader(connectionstring, stored_procedure):
    newcon = con_create(connectionstring)
    newcon = con_open(newcon)
    result_table = sp_data_reader(newcon, stored_procedure)
    newcon = con_close(newcon)
    return result_table


def sp_args_data_reader(connection, stored_procedure, input_params = dict()
                            ,output_params = dict(), return_value = dict(), timeout = None):
    """bla bla
    """
    if len(set((*input_params,*output_params,*return_value))) != (len(input_params) + len(output_params) + len(return_value)):
        raise KeyError("Duplicity in parameters, you can use unicate values.")
    cmd = connection.CreateCommand()
    if timeout:
        cmd.CommandTimeout = timeout
    cmd.CommandType = CommandType.StoredProcedure
    cmd.CommandText = stored_procedure
    param_dict = dict()
    for paramin in input_params:
        param_dict[paramin] = cmd.Parameters.Add(paramin, eval(input_params[paramin][0]))
        param_dict[paramin].Direction = ParameterDirection.Input
        param_dict[paramin].Value = input_params[paramin][1]

    for paramou in output_params:
        param_dict[paramou] = cmd.Parameters.Add(paramou, eval(output_params[paramou]))
        param_dict[paramou].Direction = ParameterDirection.Output

    for retval in return_value:
        param_dict[retval] = cmd.Parameters.Add(retval, return_value[retval])
        param_dict[retval].Direction = ParameterDirection.ReturnValue

    rdr = cmd.ExecuteReader()
    result_table = reader_to_datatable(rdr)
    rdr.Close()
    return result_table, cmd.Parameters


def con_sp_args_data_reader(connectionstring, stored_procedure, input_params = dict()
                            ,output_params = dict(), return_value = dict(), timeout = None):
    """bla bla
    """
    if len(set((*input_params,*output_params,*return_value))) != (len(input_params) + len(output_params) + len(return_value)):
        raise KeyError("Duplicity in parameters, you can use unicate values.")
    newcon = con_create(connectionstring)
    newcon = con_open(newcon)
    result_table, parameters = sp_args_data_reader(newcon, stored_procedure, input_params, output_params, return_value, timeout)
    newcon = con_close(newcon)
    return result_table, parameters


def exec_transaction(connection, transaction_name, commands):
    cmd = connection.CreateCommand()
    transaction = connection.BeginTransaction(transaction_name)
    cmd.Connection = connection
    cmd.Transaction = transaction
   
    try:
        result = 0
        for i in commands:
            cmd.CommandText = i
            result += cmd.ExecuteNonQuery()
        transaction.Commit()
    except:
        try:
            transaction.Rollback()
            return None
        except:
            print("shit")
            raise  
    return result


def con_exec_transaction(connectionstring, transaction_name, commands = list()):
    newcon = con_create(connectionstring)
    newcon = con_open(newcon)
    result = exec_transaction(newcon, transaction_name, commands)
    newcon = con_close(newcon)
    return result


def exec_query(connection, commands):
    cmd = connection.CreateCommand()
    cmd.Connection = connection
    result = 0
    for i in commands:
        cmd.CommandText = i
        result += cmd.ExecuteNonQuery()
    return result


def con_exec_query(connectionstring, commands = list()):
    newcon = con_create(connectionstring)
    newcon = con_open(newcon)
    result = exec_query(newcon, commands)
    newcon = con_close(newcon)
    return result


def printit(datatable):
    for i in datatable.Rows:
        for f in range(datatable.Columns.Count):
            print(i[f],end = "\t")
        print("\n")


def table_to_pdframe(table):
    df = pd.DataFrame([i.ItemArray for i in table.Rows])
    return df



if __name__ == "__main__":
    con_string = "Data Source=.\SQLEXPRESS;Initial Catalog=northwind;Integrated Security=True"
    querry = """SELECT TOP (1000) [ProductID]
          ,[ProductName]
          ,[SupplierID]
        ,[CategoryID]
        ,[QuantityPerUnit]
        ,[UnitPrice]
        ,[UnitsInStock]
        ,[UnitsOnOrder]
        ,[ReorderLevel]
        ,[Discontinued]
        FROM [Northwind].[dbo].[Products]"""




    result, parameters = con_sp_args_data_reader(con_string, "sp_ordersByEmployeeId2", {"@EmployeeID":["SqlDbType.Int",4],"@CustomerID":["SqlDbType.NChar","HANAR"]}
                            ,{"@OrderCount":"SqlDbType.Int"}
                            ,{"retval":"SqlDbType.Int"}
                            )

    for l in parameters:
        print(parameters[str(l)].Value)
    
    input(parameters["retval"].Value)




    result = con_pull_data_adapter(con_string, querry, "TableResult")
    df = table_to_pdframe(result)
    print(df)
    
    result = con_pull_data_reader(con_string, querry)
    df = table_to_pdframe(result)
    print(df)


   
    result = con_sp_data_reader(con_string, "sp_selectEmployeesDetails")
    
    df = table_to_pdframe(result)
    print(df)

    print("text")
    result = con_query_data_reader(con_string, querry)
    df = table_to_pdframe(result)
    print(df)
    input()


    dffd = ["Insert into Region (RegionID, RegionDescription) VALUES (118, 'Description')","Insert into Region (RegionID, RegionDescription) VALUES (119, 'Description')"]
    dffd = ["update Region set RegionID= 88 where RegionID = 88 update Region set RegionID= 818 where RegionID = 119"
            ,"update Region set RegionID= 818 where RegionID = 119"]

    dffdc = ["delete from Region where RegionID= 818"]
    
    print("text2")
    result = con_exec_transaction(con_string, "shizzzzz", dffd)
    
    print(result)
    result = con_exec_query(con_string,  dffdc)
    
    print(result)
    
    input()


