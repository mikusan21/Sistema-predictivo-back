from fastapi import APIRouter, Query, Depends
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from src.exceptions import NotFound, BadRequest
import json
import joblib
from src.data.schemas import DataModel


router = APIRouter()

DATA_DIR = Path(__file__).resolve().parents[2]/ "ml_model" / "data"
ML_MODEL_DIR = Path(__file__).resolve().parents[2] / "ml_model" / "data"/ "models"


def load_csv_data(filename: str) -> pd.DataFrame:
     file_path = DATA_DIR / filename
     if not file_path.exists():
         raise NotFound(f"Archivo no encontrado: {file_path}")
     return pd.read_csv(file_path)

def parse_date(date_str: str):
    try:
        return datetime.strptime(date_str, "%d-%m-%Y")
    except ValueError:
        raise BadRequest(f"Formato de fecha inválido: {date_str}. Debe ser DD-MM-YYYY.")    

def date_to_annomes(date_obj: datetime) -> str:
    return date_obj.strftime("%Y%m")

@router.get("/summary")
async def get_summary(
    start_date: int,
    end_date: int,
    product_type: Optional[List[str]] = Query(None),
    strategy: Optional[List[str]] = Query(None),
    real_time: bool = Query(False)
):
    try:
        tformdet_orig = load_csv_data("tformdet.csv")
        mstockalm_orig = load_csv_data("mstockalm.csv")
        mproducto_orig = load_csv_data("mproducto.csv")
        
        product_type_list = []
        if product_type:
            for item in product_type: 
                product_type_list.extend([pt.strip() for pt in item.split(',') if pt.strip()])
            product_type_list = list(set(pt for pt in product_type_list if pt)) 

        strategy_list = []
        if strategy:
            for item in strategy:
                strategy_list.extend([s.strip() for s in item.split(',') if s.strip()])
            strategy_list = list(set(s for s in strategy_list if s)) 

        mproducto = mproducto_orig.copy()
        if product_type_list:
            mproducto = mproducto[mproducto["MEDTIP"].astype(str).isin(product_type_list)]
        if strategy_list:
            mproducto = mproducto[mproducto["MEDEST"].astype(str).isin(strategy_list)]
        
        mproducto_cols_to_select = ["MEDCOD", "MEDNOM", "MEDPRES", "MEDCNC", "MEDTIP", "MEDPET", "MEDFF", "MEDEST"]
        mproducto_cols_existentes = [col for col in mproducto_cols_to_select if col in mproducto.columns]
        mproducto_unique = mproducto[mproducto_cols_existentes].drop_duplicates(subset=["MEDCOD"])

        tformdet = tformdet_orig.copy()
        if 'ANNOMES' not in tformdet.columns:
            raise HTTPException(status_code=400, detail="La columna 'ANNOMES' no existe en tformdet.csv")
        tformdet['ANNOMES'] = pd.to_numeric(tformdet['ANNOMES'], errors='coerce')
        tformdet = tformdet.dropna(subset=['ANNOMES']) 
        tformdet['ANNOMES'] = tformdet['ANNOMES'].astype(int)

        tformdet = tformdet[(tformdet["ANNOMES"] >= start_date) & (tformdet["ANNOMES"] <= end_date)]

        num_unique_anomes = 0
        if not tformdet.empty:
            num_unique_anomes = len(tformdet["ANNOMES"].unique())
                
        if tformdet.empty:
            return {"count": 0, "data_count": 0, "anomes": 0, "months": [], "data": []}

        consumo_cols = ["VENTA", "SIS", "INTERSAN"]
        for col in consumo_cols:
            if col not in tformdet.columns:
                raise HTTPException(status_code=400, detail=f"La columna '{col}' requerida para TOTAL_CONSUMO no existe en tformdet.csv")
            tformdet[col] = pd.to_numeric(tformdet[col], errors='coerce').fillna(0) 

        tformdet["TOTAL_CONSUMO"] = tformdet[consumo_cols].sum(axis=1)
        
        if product_type_list or strategy_list:
            if not mproducto_unique.empty and "MEDCOD" in mproducto_unique.columns:
                 tformdet = tformdet.merge(mproducto_unique[["MEDCOD"]], left_on="CODIGO_MED", right_on="MEDCOD", how="inner")
            else:
                 tformdet = pd.DataFrame(columns=tformdet.columns) 
            
            if tformdet.empty:
                return {"count": 0, "data_count": 0, "anomes": num_unique_anomes, "months": [], "data": []}

        if tformdet.empty or "CODIGO_MED" not in tformdet.columns: 
            consumo_pivot = pd.DataFrame() 
        else:
            consumo_mensual = tformdet.groupby(["CODIGO_MED", "ANNOMES"])["TOTAL_CONSUMO"].sum().reset_index()
            if consumo_mensual.empty:
                consumo_pivot = pd.DataFrame()
            else:
                consumo_pivot = consumo_mensual.pivot(index="CODIGO_MED", columns="ANNOMES", values="TOTAL_CONSUMO").fillna(0)

        month_columns_numeric = [col for col in consumo_pivot.columns if isinstance(col, (int, np.integer, float, np.floating))]
        
        if consumo_pivot.empty or not month_columns_numeric: 
            if consumo_pivot.empty and "CODIGO_MED" in tformdet.columns: 
                 unique_codigos_med = tformdet["CODIGO_MED"].unique()
                 consumo_pivot = pd.DataFrame(index=pd.Index(unique_codigos_med, name="CODIGO_MED"))
            consumo_pivot["CPMA"] = 0.0
            consumo_pivot["CONSUMO_MEN"] = 0
        else:
            consumo_pivot["CPMA"] = consumo_pivot[month_columns_numeric].mean(axis=1)
            consumo_pivot["CONSUMO_MEN"] = (consumo_pivot[month_columns_numeric] > 0).sum(axis=1)
        
        months_for_output = sorted([str(col) for col in month_columns_numeric]) 
        
        if "CODIGO_MED" not in consumo_pivot.index.names and "CODIGO_MED" not in consumo_pivot.columns:
             consumo_pivot = consumo_pivot.reset_index(drop=True) 
        else:
            consumo_pivot = consumo_pivot.reset_index()

        if real_time:
            stock_df = mstockalm_orig.copy()
            if not stock_df.empty and "MEDCOD" in stock_df.columns and "STKSALDO" in stock_df.columns:
                stock_df['STKSALDO'] = pd.to_numeric(stock_df['STKSALDO'], errors='coerce').fillna(0)
                stock_to_use = stock_df.groupby("MEDCOD", as_index=False)["STKSALDO"].sum()
                stock_to_use = stock_to_use.rename(columns={"MEDCOD": "CODIGO_MED", "STKSALDO": "STOCK_FIN"})
                if "CODIGO_MED" in consumo_pivot.columns:
                     consumo_pivot = consumo_pivot.merge(stock_to_use, on="CODIGO_MED", how="left")
                else: 
                     consumo_pivot["STOCK_FIN"] = pd.NA
            else:
                consumo_pivot["STOCK_FIN"] = pd.NA
        else:
            if not tformdet.empty and "CODIGO_MED" in tformdet.columns and "STOCK_FIN" in tformdet.columns:
                tformdet['STOCK_FIN'] = pd.to_numeric(tformdet['STOCK_FIN'], errors='coerce').fillna(0)
                latest_stock_fin_in_period = tformdet.sort_values("ANNOMES", ascending=False)\
                                                 .drop_duplicates(subset=["CODIGO_MED"], keep="first")\
                                                 [["CODIGO_MED", "STOCK_FIN"]]
                if "CODIGO_MED" in consumo_pivot.columns:
                    consumo_pivot = consumo_pivot.merge(latest_stock_fin_in_period, on="CODIGO_MED", how="left")
                else:
                    consumo_pivot["STOCK_FIN"] = pd.NA
            else:
                consumo_pivot["STOCK_FIN"] = pd.NA

        if "STOCK_FIN" not in consumo_pivot.columns:
            consumo_pivot["STOCK_FIN"] = pd.NA
        consumo_pivot["STOCK_FIN"] = pd.to_numeric(consumo_pivot["STOCK_FIN"], errors='coerce').fillna(0.0)

        if "CPMA" not in consumo_pivot.columns: consumo_pivot["CPMA"] = 0.0
        consumo_pivot["CPMA"] = pd.to_numeric(consumo_pivot["CPMA"], errors='coerce').fillna(0.0)

        consumo_pivot["NIVELES"] = consumo_pivot["STOCK_FIN"] / consumo_pivot["CPMA"]
        consumo_pivot["NIVELES"] = consumo_pivot["NIVELES"].replace([np.inf, -np.inf], np.nan)
        consumo_pivot["NIVELES"] = consumo_pivot["NIVELES"].fillna(0.0)

        def situacion(nivel, cpma, stock_fin):
            if pd.isna(nivel): return "Indeterminado" 
            if cpma == 0:
                return "Sobrestock (Sin Consumo)" if stock_fin > 0 else "Normostock (Sin Movimiento)" 
            if nivel > 7: return "Sobrestock"
            elif nivel < 1: return "Substock"
            else: return "Normostock"

        if not consumo_pivot.empty :
            consumo_pivot["SITUACION"] = consumo_pivot.apply(lambda row: situacion(row["NIVELES"], row["CPMA"], row["STOCK_FIN"]), axis=1)
        else:
            consumo_pivot["SITUACION"] = None 

        if not consumo_pivot.empty and "CODIGO_MED" in consumo_pivot.columns and not mproducto_unique.empty and "MEDCOD" in mproducto_unique.columns:
            consumo_pivot = consumo_pivot.merge(mproducto_unique, left_on="CODIGO_MED", right_on="MEDCOD", how="left", suffixes=('', '_mprod'))
            if 'MEDCOD_mprod' in consumo_pivot.columns:
                consumo_pivot = consumo_pivot.drop(columns=['MEDCOD_mprod'])
        else: 
            for col in mproducto_cols_to_select:
                if col != "MEDCOD" and col not in consumo_pivot.columns: 
                    consumo_pivot[col] = None


        final_df_data = {}
        base_cols = ["CODIGO_MED", "CPMA", "CONSUMO_MEN", "STOCK_FIN", "NIVELES", "SITUACION"]
        
        if consumo_pivot.empty: 
            final_df = pd.DataFrame(columns=base_cols + mproducto_cols_to_select[1:] + months_for_output) 
        else:
            product_attr_cols_final = [col for col in mproducto_cols_to_select if col != "MEDCOD" and col in consumo_pivot.columns]
            ordered_columns_for_selection = ["CODIGO_MED"] + \
                                    month_columns_numeric + \
                                    ["CPMA", "CONSUMO_MEN", "STOCK_FIN", "NIVELES", "SITUACION"] + \
                                    product_attr_cols_final
            
            existing_cols_for_selection = [col for col in ordered_columns_for_selection if col in consumo_pivot.columns]
            final_df = consumo_pivot[existing_cols_for_selection].copy() 
            rename_map = {num_col: str(num_col) for num_col in month_columns_numeric if num_col in final_df.columns}
            final_df.rename(columns=rename_map, inplace=True)
            for month_str in months_for_output:
                if month_str not in final_df.columns:
                    final_df[month_str] = 0.0 

            final_ordered_cols_with_str_months = ["CODIGO_MED"] + \
                                   months_for_output + \
                                   ["CPMA", "CONSUMO_MEN", "STOCK_FIN", "NIVELES", "SITUACION"] + \
                                   product_attr_cols_final
            
            for col_name in final_ordered_cols_with_str_months:
                if col_name not in final_df.columns:
                    if col_name in base_cols or col_name in months_for_output: 
                        final_df[col_name] = 0.0
                    else: 
                        final_df[col_name] = None 
            
            final_df = final_df[final_ordered_cols_with_str_months] 

        descriptive_text_cols = ["MEDNOM", "MEDPRES", "MEDCNC", "MEDTIP", "MEDPET", "MEDFF", "MEDEST"]
        for col in descriptive_text_cols:
            if col in final_df.columns:
                final_df[col] = final_df[col].fillna("Desconocido")
        
        final_df = final_df.replace({np.nan: None})
        
        for month_str_col in months_for_output:
            if month_str_col in final_df.columns:
                final_df[month_str_col] = final_df[month_str_col].fillna(0.0)


        data_output = json.loads(final_df.to_json(orient="records", date_format="iso"))
        return {
            "count": len(final_df),
            "data_count": len(data_output), 
            "anomes": num_unique_anomes, 
            "months": months_for_output, 
            "data": data_output
        }
    except ValueError as e:
        raise BadRequest(detail=f"Error en formato de fecha: {str(e)}")
    except Exception as e:
        raise BadRequest(detail=f"Error al procesar datos: {str(e)}")
    except FileNotFoundError as e:
        raise NotFound(detail=f"Error: Archivo CSV no encontrado - {str(e)}")
    

@router.post('/predict')
async def predict(data: DataModel):
    codigo_med, stock_fin, dates, price = data.model_dump().values()
    if (codigo_med is None): return NotFound(detail="El código del medicamento no puede ser nulo.")
    if (stock_fin is None): return NotFound(detail="El stock final no puede ser nulo.")
    if (dates is None or len(dates) == 0): return NotFound(detail="La lista de fechas no puede ser nula o vacía.")
    if (price is None): return NotFound(detail="El precio no puede ser nulo.")

    model = joblib.load(ML_MODEL_DIR /'model_xgboost.pkl')
    results = []
    for ds in dates:
        date = pd.to_datetime(ds, format='%m-%Y')

        row = {
            'STOCK_FIN': stock_fin,
            'PRECIO': price,
            'CODIGO_MED': codigo_med,
            'month': date.month,
            'year': date.year
        }
        try:
            x = pd.DataFrame([row])
            x['CODIGO_MED'] = x['CODIGO_MED'].astype('category')
            x = pd.get_dummies(x, columns=['CODIGO_MED'], prefix='MED')
            x = x.reindex(columns=model.get_booster().feature_names, fill_value=0)
            y_pred = model.predict(x)[0]
            y_pred = max(0, y_pred)

            results.append({
                'date': ds,
                'prediction': round(float(y_pred), 2)
            })
        
        except FileNotFoundError:
            raise NotFound(detail="El modelo no se encuentra disponible.")
        
    return {
            'codigo_med': codigo_med,
            'prediction': results
        }