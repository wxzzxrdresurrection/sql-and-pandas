import pandas as pd
from config import DB
from consulta2 import empleados_por_region, convert_to_excel

def ganancias_por_region(db, query, er):
    result = db.select(query)
    #Unir las tablas
    join = pd.merge(result, er, on="EmployeeID", how="inner")
    #Calcular el total del cliente
    join["Ganancia"] = join["UnitPrice"] * join["Quantity"]
    #Columnas necesarias
    join = join[["RegionDescription", "Ganancia"]]
    #Agrupar por región
    grouped = join.groupby(["RegionDescription"]).sum().reset_index()
    print(grouped)
    return grouped


if __name__ == '__main__':
    db = DB("localhost", "root", "admin", "northwind")

    query = """
        SELECT * FROM Region r 
            JOIN Territories t ON r.RegionID = t.RegionID
            JOIN EmployeeTerritories et ON t.TerritoryID = et.TerritoryID
    """

    data = empleados_por_region(db, query)

    query2 = """SELECT * FROM Orders o
            JOIN `Order Details`od ON o.OrderID = od.OrderID
    """

    ganancias = ganancias_por_region(db, query2, data)

    convert_to_excel(ganancias, "consulta3", "ganancias_por_region")


