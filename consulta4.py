from config import DB
import pandas as pd
from tabulate import tabulate

def productos_mas_vendidos_por_categoria(db, query):
    result = db.select(query)
    #Eliminar columnas duplicadas
    result = result.loc[:,~result.columns.duplicated()]
    #Sacamos el año
    result["Year"] = result["OrderDate"].dt.year
    #Calcular el total del producto
    result["Sales"] = result["UnitPrice"] * result["Quantity"]
    #Columnas necesarias
    result = result[["Year", "CategoryName", "ProductName", "Sales"]]
    #Agrupacion por año, categoria y producto
    result = result.groupby(["Year", "CategoryName", "ProductName"]).sum().reset_index()
    #Ordenar por categoría y total de cliente
    print(result)
    return result

def rankear_ventas_y_enumerar_years(df):
    #Ranking de ventas por año y categoria
    df = df.assign(RankVenta=df.groupby(["Year", "CategoryName"])["Sales"].rank(ascending=False))
    #Enumerar los años
    df["YearRank"] = df.Year.rank(method="dense", ascending=False).astype(int) 
    #Ordenar por año, categoria y total de cliente
    df = df.sort_values(by=["Year", "CategoryName", "Sales"], ascending=[False, True, False])
    print(df)
    return df

def producto_mas_comprado_por_categoria_y_ultimos_years(df):
    #Filtrar por el producto más vendido por categoria y los ultimos 3 años
    df = df[df["RankVenta"] == 1]
    df = df[df["YearRank"].between(1, 3)]
    #Resetear indices
    df.reset_index(drop=True, inplace=True)
    print(df)
    return df

def productos_comprados_clientes(db, query):
    result = db.select(query)
    #Eliminar columnas duplicadas
    result = result.loc[:,~result.columns.duplicated()]
    #Sacamos el año
    result["Year"] = result["OrderDate"].dt.year
    #Calcular el total del producto
    result["Sales"] = result["UnitPrice"] * result["Quantity"]
    #Columnas necesarias
    result = result[["Year", "CustomerID", "CompanyName", "ProductName", "Sales"]]
    #Agrupacion por año, categoria y producto
    result = result.groupby(["Year", "ProductName", "CustomerID", ]).sum().reset_index()
    #Ordenar por categoría y total de cliente
    result = result.sort_values(by=["Year", "ProductName", "Sales"], ascending=[False, True, False])
    print(result)
    return result

def clientes_productos_mas_vendidos_categoria(df, df2):
    #Unir los dataframes
    df = pd.merge(df, df2, on=["ProductName", "Year"])
    #Ranking de ventas por año y producto descendentemente
    df = df.assign(RankDesc=df.groupby(["Year", "ProductName"])["Sales_y"].rank(ascending=False, method="dense"))
    #Ranking de ventas por año y producto ascendentemente
    df = df.assign(RankAsc=df.groupby(["Year", "ProductName"])["Sales_y"].rank(ascending=True, method="dense"))
    print(df)
    df.to_excel("productos_clientes.xlsx", index=False)
    return df

def filtro_producto_mas_y_menos_comprado(df):
    #Filtrar el producto más y menos comprado por cliente
    df = df[(df["RankDesc"] == 1) | (df["RankAsc"] == 1)]
    #Columnas necesarias
    df = df[["YearRank", "Year", "CategoryName", "ProductName", "CompanyName", "Sales_y"]]
    print(df)
    return df
    

if __name__ == "__main__":
    db = DB("localhost", "root", "admin", "northwind")

    query = """SELECT * FROM `Order Details` od 
        JOIN Orders o ON od.OrderID = o.OrderID
        JOIN Products p ON od.ProductID = p.ProductID 
        JOIN Categories c ON p.CategoryID = c.CategoryID
    """

    productos = productos_mas_vendidos_por_categoria(db, query)

    productos_rankeados = rankear_ventas_y_enumerar_years(productos)

    productos_ultimos_años = producto_mas_comprado_por_categoria_y_ultimos_years(productos_rankeados)

    query2 = """SELECT * FROM Orders o
        JOIN `Order Details` od ON o.OrderID = od.OrderID
        JOIN Customers c ON o.CustomerID = c.CustomerID
        JOIN Products p ON od.ProductID = p.ProductID
    """

    productos_clientes = productos_comprados_clientes(db, query2)

    join_productos = clientes_productos_mas_vendidos_categoria(productos_ultimos_años, productos_clientes)

    filtro_producto_mas_y_menos_comprado(join_productos)

