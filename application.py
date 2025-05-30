from flask import Flask, render_template, jsonify, Response
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler

import yfinance as yf
import pandas as pd
from datetime import datetime

from sqlalchemy import create_engine, inspect, Column, String, Integer, MetaData, Table, DateTime, Date, Float
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, backref, scoped_session
from sqlalchemy import desc, and_

import json
import os

app = Flask(__name__)
CORS(app)

# Obtenir le chemin absolu du répertoire contenant le script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construire le chemin complet vers le fichier JSON
json_path = os.path.join(script_dir, 'param', 'param.json')

# Ouvrir et lire le fichier JSON
with open(json_path, 'r', encoding='utf-8') as file:
    data = json.load(file)

scheduler = BackgroundScheduler()

file_path = os.path.join(script_dir, 'data inputs', '20241206_comptes action opérations.xlsx')


# Construire le chemin complet vers le fichier de la base de données SQLite
db_path = os.path.join(script_dir, 'data outputs', 'PatrimoineBoursier.db')

# Créer l'URL de connexion pour SQLAlchemy
db_url = f'sqlite:///{db_path}'

# Créer le moteur SQLAlchemy
engine = create_engine(db_url)


# Définir une base pour les modèles
Base = declarative_base()

class ValeurMarche(Base):
    __tablename__ = 'valeur_marche'

    Date = Column(Date, primary_key=True)
    Close = Column(Float)
    Ticker = Column(String, primary_key=True)

# Crée la table dans la base de données
Base.metadata.create_all(engine)

# Création d'une session scoped
SessionSc = scoped_session(sessionmaker(bind=engine))

def UpdateDataAsset(file_path):
    session = SessionSc()  # Récupération de la session isolée

    dfOperations = pd.read_excel(file_path, sheet_name='opérations')

    dfOperations ['Date de valeur'] = pd.to_datetime(dfOperations ['Date de valeur'])

    ListeSymbole = dfOperations.groupby("Symbole", dropna=True)["Date de valeur"].min().reset_index()

    def get_most_recent_date(ticker):
        result = (session.query(ValeurMarche.Date)
                        .filter_by(Ticker=ticker)
                        .order_by(desc(ValeurMarche.Date))
                        .first())
        return result.Date if result else None

    ListeSymbole['Date DB'] = ListeSymbole['Symbole'].apply(get_most_recent_date)

    # Obtenir la date et l'heure actuelles
    now = datetime.now()

    # Extraire uniquement la date
    today_date = now.date()

    for index, row in ListeSymbole.iterrows():
        # Déterminer la date de début
        start_date = row["Date DB"] if pd.notna(row["Date DB"]) else row["Date de valeur"]
        start_date = pd.to_datetime(start_date, errors="coerce")  # Convertir en date

        # Télécharger les données de marché pour le symbole donné
        dfValeurMarcheTemp = yf.download(row["Symbole"], start=start_date, end=today_date)

        if dfValeurMarcheTemp.empty:
            continue

        # Sélectionner uniquement la colonne 'Close' et réinitialiser l'index
        dfValeurMarcheTemp = dfValeurMarcheTemp[["Close"]].reset_index()
        dfValeurMarcheTemp['Ticker'] = row["Symbole"]
        dfValeurMarcheTemp.columns = ['Date', 'Close', 'Ticker']

        # Insérer les données dans la base de données
        for _, data_row in dfValeurMarcheTemp.iterrows():
            stock_price = ValeurMarche(
                Date=data_row['Date'],
                Close=data_row['Close'],
                Ticker=data_row['Ticker']
            )
            SessionSc .merge(stock_price)  # Utilise merge pour éviter les doublons
        SessionSc.commit()  # Valide les insertions dans la base de données

    data = {
        'Symbole': [],
        'Date': [],
        'Close': [],
        'Nombre de parts': [],
        'Montant investi euro': [],
        'Valeur marché': []
    }

    for index, row in ListeSymbole.iterrows():
        resultats = session.query(ValeurMarche).filter(
            and_(
                ValeurMarche.Ticker == row['Symbole'],
                ValeurMarche.Date >= row['Date de valeur']
            )
        ).all()

        for resultat in resultats:

            data['Symbole'].append(resultat.Ticker)
            data['Date'].append(resultat.Date)
            data['Close'].append(resultat.Close)

            # Filtrer dfOperations selon les conditions spécifiées
            date_seuil_timestamp = pd.Timestamp(resultat.Date)
            filtres = (dfOperations['Date de valeur'] <= date_seuil_timestamp) & (dfOperations['Symbole'] == resultat.Ticker)
            nombre_parts = dfOperations.loc[filtres, 'Nombre de parts'].sum()
            data['Nombre de parts'].append(nombre_parts)

            data['Valeur marché'].append(nombre_parts*resultat.Close)

            filtresMontantEuro = (dfOperations['Date de valeur'] <= date_seuil_timestamp) & (dfOperations['Symbole'] == resultat.Ticker)& (dfOperations['Type opération'].isin(['Versement libre complémentaire', 'Désinvestissement']))
            sommeeuro = dfOperations.loc[filtresMontantEuro, 'Montant net en euros'].sum()
            data['Montant investi euro'].append(sommeeuro)

    dfPerformance = pd.DataFrame(data)
    # Construire le chemin complet vers le fichier de la base de données SQLite
    CMcsv_path = os.path.join(script_dir, 'data outputs', 'Cours_Marchés.csv')
    dfPerformance.to_csv(CMcsv_path, index=False)
    
    dfValeurTotal = dfPerformance.groupby('Date')[['Valeur marché', 'Montant investi euro']].sum()
    VTcsv_path = os.path.join(script_dir, 'data outputs', 'ValeurMarcheJour.csv')
    dfValeurTotal.to_csv(VTcsv_path, index=True)

@app.route('/api/valeurmarche', methods=['GET'])
def get_valeurmarchejour_data():
    try:

        script_dir = os.path.dirname(os.path.abspath(__file__))
        VTcsv_path = os.path.join(script_dir, 'data outputs', 'ValeurMarcheJour.csv')
        dfValeurTotal = pd.read_csv(VTcsv_path)

        # Convertir le DataFrame en JSON avec l'orientation 'records'
        valeurmarche_json = dfValeurTotal.to_json(orient='records', date_format='iso')

        # Retourner la réponse JSON avec le type MIME approprié
        return Response(valeurmarche_json, mimetype='application/json')
    
    except Exception as e:
        return Response(f"Erreur lors de la lecture des données : {e}", status=500)

@app.route('/api/valeurmarche/last', methods=['GET'])
def get_valeurmarchejour_last():
    """Retourne la dernière ligne des données de valeur du marché sous forme de JSON."""
    # Obtenir le chemin absolu du répertoire contenant le script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Construire le chemin complet vers le fichier CSV
    VTcsv_path = os.path.join(script_dir, 'data outputs', 'ValeurMarcheJour.csv')

    # Lire le fichier CSV dans un DataFrame
    dfValeurTotal = pd.read_csv(VTcsv_path)

    # Extraire la dernière ligne du DataFrame
    last_row = dfValeurTotal.iloc[-1]

    # Convertir la dernière ligne en dictionnaire
    last_row_dict = last_row.to_dict()

    # Retourner la réponse JSON avec le type MIME approprié
    return jsonify(last_row_dict)

@app.route('/')
def hello():
    return 'API de gestion de portefeuille boursier'

if __name__ == '__main__':
    scheduler.add_job(func=UpdateDataAsset, trigger='cron', hour=5, args=[file_path])
    scheduler.start()
    app.run(host='0.0.0.0', port=5077, debug=True)