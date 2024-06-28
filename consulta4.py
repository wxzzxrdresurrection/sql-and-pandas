from config import DB
import pandas as pd
import re
from consulta2 import convert_to_excel

def productos_mas_vendidos_por_categoria(db, query):
    result = db.select(query)
    #Eliminar columnas duplicadas
    result = result.loc[:,~result.columns.duplicated()]
    #Sacamos el año
    result["Year"] = result["OrderDate"].dt.year
    #Calcular el total del producto
    result["SalesProduct"] = result["UnitPrice"] * result["Quantity"]
    #Columnas necesarias
    result = result[["Year", "CategoryName", "ProductName", "SalesProduct"]]
    #Agrupacion por año, categoria y producto
    result = result.groupby(["Year", "CategoryName", "ProductName"]).sum().reset_index()
    #Ordenar por categoría y total de cliente
    print(result)
    return result

def rankear_ventas_y_enumerar_years(df):
    #Ranking de ventas por año y categoria
    df = df.assign(RankVenta=df.groupby(["Year", "CategoryName"])["SalesProduct"].rank(ascending=False))
    #Enumerar los años
    df["YearRank"] = df.Year.rank(method="dense", ascending=False).astype(int) 
    #Ordenar por año, categoria y total de cliente
    df = df.sort_values(by=["Year", "CategoryName", "SalesProduct"], ascending=[False, True, False])
    print(df)
    return df

def producto_mas_comprado_por_categoria_y_ultimos_years(df):
    #Filtrar por el producto más vendido por categoria y los ultimos 3 años
    df = df[df["RankVenta"] == 1]
    df = df[df["YearRank"].between(1, 3)]
    #Resetear indices
    df.reset_index(drop=True, inplace=True)
    #Columnas necesarias
    df = df[["YearRank" ,"Year", "CategoryName", "ProductName", "SalesProduct"]]
    print(df)
    return df

def productos_comprados_clientes(db, query):
    result = db.select(query)
    #Eliminar columnas duplicadas
    result = result.loc[:,~result.columns.duplicated()]
    #Sacamos el año
    result["Year"] = result["OrderDate"].dt.year
    #Calcular el total del producto
    result["SalesCustomer"] = result["UnitPrice"] * result["Quantity"]
    #Columnas necesarias
    result = result[["Year", "CustomerID", "CompanyName", "ProductName", "SalesCustomer"]]
    #Agrupacion por año, categoria y producto
    result = result.groupby(["Year", "CustomerID", "ProductName"]).sum().reset_index()
    #Ordenar por categoría y total de cliente
    result = result.sort_values(by=["Year", "ProductName", "SalesCustomer"], ascending=[False, True, False])
    result.reset_index(drop=True, inplace=True)
    print(result)
    return result

def clientes_productos_mas_vendidos_categoria(df, df2):
    #Unir los dataframes
    join = df.join(df2.set_index(["Year", "ProductName"]), on=["Year", "ProductName"], how="inner")
    #Distinct de CompanyName
    join["CompanyName"] = join["CompanyName"].apply(remove_repeated_patterns)
    #Ranking de ventas por año y producto descendentemente
    join["RankDesc"] = join.SalesCustomer.rank(ascending=False, method="dense")
    join["RankDesc"] = join.groupby(["Year", "ProductName"])["RankDesc"].rank(ascending=True, method="dense")
    #Ranking de ventas por año y producto ascendentemente
    join["RankAsc"] = join.SalesCustomer.rank(ascending=True, method="dense")
    join["RankAsc"] = join.groupby(["Year", "ProductName"])["RankAsc"].rank(ascending=True, method="dense")
    print(join)
    return join

def remove_repeated_patterns(cell_value):
    if pd.isna(cell_value):
        return cell_value
    
    # Buscar patrones repetidos en la cadena usando regex
    pattern = re.compile(r'(.+?)\1+')
    match = pattern.match(cell_value)
    if match:
        return match.group(1)
    return cell_value

def filtro_producto_mas_y_menos_comprado(df):
    #Filtrar el producto más y menos comprado por cliente
    df = df[(df["RankDesc"] == 1) | (df["RankAsc"] == 1)]
    #Resetear indices
    df.reset_index(drop=True, inplace=True)
    #Concatenar nombre de la empresa y ganancia
    df["CompanyName"] = df["CompanyName"] + " = $" + df["SalesCustomer"].astype(str)
    #Columnas necesarias
    df = df[["YearRank" ,"Year", "CategoryName", "ProductName", "CompanyName", "SalesCustomer"]]
    print(df)
    return df

def change_years(value):
    if value == 1:
        return "Último"
    elif value == 2:
        return "Penúltimo"
    else:
        return "Antepenúltimo"

def matriz(df):
    #Columnas necesarias 
    df = df[["YearRank", "Year", "CategoryName", "ProductName", "CompanyName"]]
    #Agrupar con categorias y años
    df = df.groupby(['CategoryName', 'YearRank']).apply(lambda x: ', '.join(x.apply(lambda row: f"Producto: {row['ProductName']} Clientes: {row['CompanyName']}", axis=1))).reset_index(name='Productos y Clientes')
    #Generar la matriz
    df = df.pivot_table(index='CategoryName', columns='YearRank', values='Productos y Clientes', aggfunc='first').reset_index()
    #Renombrar los indexes
    year_mapping = {1: "Último", 2: "Penúltimo", 3: "Antepenúltimo"}
    df = df.rename(columns=year_mapping)
    print(df)
    convert_to_excel(df, "consulta4", "categorias con el cliente que más y menos compró")
    #return
    

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

    categoria_con_clientes = filtro_producto_mas_y_menos_comprado(join_productos)

    matriz(categoria_con_clientes)

