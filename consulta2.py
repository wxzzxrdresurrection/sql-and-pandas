import pandas as pd
from config import DB

def empleados_por_region(db, query):
    result = db.select(query)
    #DISTINCT de los resultados
    result = result.drop_duplicates(subset=["EmployeeID"])
    #Columnas necesarias
    result = result[["RegionID", "RegionDescription", "EmployeeID"]]
    #Eliminar las columnas duplicadas
    result = result.loc[:,~result.columns.duplicated()]
    #Recortar cadena vacia
    result["RegionDescription"] = result["RegionDescription"].str.strip()
    #Imprimir resultado
    print(result)
    return result

def total_clientes_year_y_region(query, subquery, db):
    result = db.select(query)
    #Unir las tablas
    join = pd.merge(result, subquery, on="EmployeeID", how="inner")
    #Extraer año
    join["Year"] = join["OrderDate"].dt.year
    #Calcular el total del cliente
    join["TotalCustomer"] = join["UnitPrice"] * join["Quantity"]
    #Columnas necesarias
    join = join[["Year", "RegionDescription", "TotalCustomer", "CustomerID"]]
    #Agrupar por año y región
    grouped = join.groupby(["Year","RegionDescription", "CustomerID"]).sum().reset_index()
    #Solo las columnas necesarias
    grouped = grouped[["Year", "RegionDescription", "TotalCustomer", "CustomerID"]]
    #Ordenar por año, región y total de orden
    grouped = grouped.sort_values(by=["Year", "RegionDescription", "TotalCustomer"], ascending=[False, True, False])
    print(grouped)
    return grouped

def rankear_ventas(df):
    clientes = """SELECT * FROM Customers"""
    clientes = db.select(clientes)
    #Solo las columnas necesarias
    clientes = clientes[["CustomerID", "CompanyName"]]
    #Unir las tablas
    ventas_con_clientes = pd.merge(df, clientes, on="CustomerID", how="inner")
    #Solo las columnas necesarias
    ventas_con_clientes = ventas_con_clientes[["Year", "RegionDescription", "CompanyName", "TotalCustomer"]]
    #Mayores ventas por año y región
    ventas_con_clientes = ventas_con_clientes.assign(RankVenta=ventas_con_clientes.groupby(["Year", "RegionDescription"])["TotalCustomer"].rank(ascending=False)).query("RankVenta == 1")
    #Ordenar por año, región y total de cliente
    ventas_con_clientes = ventas_con_clientes.sort_values(by=["Year", "RegionDescription", "TotalCustomer"], ascending=[False, True, False])
    #Solo las columnas necesarias
    ventas_con_clientes = ventas_con_clientes[["Year", "RegionDescription", "CompanyName", "TotalCustomer"]]
    print(ventas_con_clientes)
    return ventas_con_clientes

def convert_to_excel(df, nombre, hoja):
    df.to_excel(f"{nombre}.xlsx", sheet_name=hoja)
    
if __name__ == '__main__':
    db = DB("localhost", "root", "admin", "northwind")

    query = """
        SELECT * FROM Region r 
            JOIN Territories t ON r.RegionID = t.RegionID
            JOIN EmployeeTerritories et ON t.TerritoryID = et.TerritoryID
    """
    
    empleados_region = empleados_por_region(db, query)

    query2 = """
        SELECT * FROM Orders o
            JOIN `Order Details`od ON o.OrderID = od.OrderID
    """

    ventas_region_año = total_clientes_year_y_region(query2, empleados_region, db)

    ventas_rankeadas = rankear_ventas(ventas_region_año)
    
    convert_to_excel(ventas_rankeadas, "consulta2", "ventas_por_region_y_año")

