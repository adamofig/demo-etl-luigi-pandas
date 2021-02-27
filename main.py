import luigi
from luigi import Task, LocalTarget, WrapperTask
import pandas as pd


class GetAllTamalesSales(Task):
    def output(self):
        return luigi.LocalTarget('processed/total_sales.csv')

    def run(self):
        # 1) Obteniendo los archivos 
        urlFiles = "source/tamales_inc/ventas_mensuales_tamales_inc/mx/20200801/csv/"
        urlCentro = f"{urlFiles}/Centro/ventas_mensuales_Centro.csv"
        urlPrivados = f"{urlFiles}/E._Privados/ventas_mensuales_E._Privados.csv"
        urlNorte = f"{urlFiles}/Norte/ventas_mensuales_Norte.csv"
        urlSur = f"{urlFiles}/Sur/ventas_mensuales_Sur.csv"
        
        headers = ["year", "month", "country", "calorie_category", "flavor", "zone", "product_code", "product_name", "sales"]
        df_ventas_centro = pd.read_csv(urlCentro, names=headers)
        df_ventas_privado = pd.read_csv(urlPrivados, names=headers)
        df_ventas_norte = pd.read_csv(urlNorte, names=headers)
        df_ventas_sur = pd.read_csv(urlSur, names=headers)

        # 2) Conjuntando información en un único archivo
        total_sales = pd.concat([df_ventas_centro, df_ventas_privado, df_ventas_norte, df_ventas_sur])
        total_sales.to_csv(self.output().path, index=False, encoding='utf-8')
        # df_ventas_centro.to_csv(f,encoding = 'utf-8',index=False,header=True,quoting=2)
        # f.close()
        # with self.output().open('w') as f:
        #     df_ventas_centro.to_csv(f, encoding = 'utf-8', index=False, header=True,quoting=2)
        #     f.close()
    
class MonthlySales(Task):

    def requires(self):
        return GetAllTamalesSales()
    
    def output(self):
        return luigi.LocalTarget('processed/monthly_sales.csv')
    
    def run(self):
        total_sales_df = pd.read_csv(self.input().path)

        # Funciones de agregación para calcular las ventas mensuales
        grouped_month = total_sales_df.groupby(["year","month"], as_index=False)
        total_monthly_sales = grouped_month[["sales"]].sum()
        # Ordenando el archivo por meses
        month_map = {'Jan' : 1, 'Feb' : 2, 'Mar' : 3, 'Apr': 4, 'May': 5, 'Jun':  6,'Jul':  7 ,'Aug':  8, 'Sep': 9,'Oct':  10,'Nov':  11, 'Dec': 12 }
        total_monthly_sales['month_num'] = total_monthly_sales['month'].map(month_map)
        total_monthly_sales_sorted = total_monthly_sales.sort_values(["year", "month_num"])

        total_monthly_sales_sorted[['year','month','sales']].to_csv(self.output().path, index=False, encoding='utf-8')


class CumulativeSales(Task):
    def requires(self):
        return MonthlySales()
    
    def output(self):
        return luigi.LocalTarget('processed/cumulative_sales.csv')
    
    def run(self):
            monthly_sales_df = pd.read_csv(self.input().path)
            monthly_sales_df['cumulative'] = monthly_sales_df.groupby("year")["sales"].cumsum()
            monthly_sales_df[['year','month','cumulative']].to_csv(self.output().path, index=False, encoding='utf-8')


class PercentageByMonthSales(Task):
    def requires(self):
        return MonthlySales()
    
    def output(self):
        return luigi.LocalTarget('processed/percentage_month_sales.csv')
    
    def run(self):
            monthly_sales_df = pd.read_csv(self.input().path)
            monthly_sales_df["percentage"] = (monthly_sales_df["sales"] - monthly_sales_df.shift(fill_value=0)["sales"]) / monthly_sales_df["sales"] * 100
            monthly_sales_df.to_csv(self.output().path, index=False, encoding='utf-8')



class Final(WrapperTask):
    def requires(self):
        return [PercentageByMonthSales() , CumulativeSales() ]

luigi.run(['Final', '--local-scheduler'])

    

