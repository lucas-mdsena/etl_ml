# BIBLIOTECAS
import pandas as pd
import sqlalchemy
import lightgbm
import os
import scikitplot as skplt
from sklearn import model_selection
from sklearn import tree
from sklearn import pipeline
from sklearn import metrics
from feature_engine import imputation

pd.set_option('display.max_rows', 100)



# LEITURA DA ABT
ML_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.dirname(ML_DIR)
ROOT_DIR = os.path.dirname(SOURCE_DIR)
DATA_DIR = os.path.join(ROOT_DIR, 'data')
DB_PATH = os.path.join(DATA_DIR, 'olist.db')
MODEL_DIR = os.path.join(ROOT_DIR, 'models')

engine = sqlalchemy.create_engine(f'sqlite:///{DB_PATH}')

with engine.connect() as con:
    df = pd.read_sql_table('abt_olist_churn', con)

df['dtReference'] = pd.to_datetime(df['dtReference'], format='%Y-%m-%d')



# SAMPLE OUT OF TIME
# Base out of time
df_oot = df[df['dtReference'] >= '2018-01-01']
print(f'Shape base oot:{df_oot.shape}')


# Base de treino
df_train = df[df['dtReference'] < '2018-01-01']
print(f'Shape base treino:{df_train.shape}')



# DEFININDO VARIÁVEIS
var_identity = ['dtReference', 'idVendedor', 'dtProxPedido']
target = 'flChurn'
to_remove = ['qtdRecencia', target] + var_identity
# Foi removida a feature qtdRecencia para o modelo, por ser praticamente a variável resposta.
# Pela sua alta correlação com a target, pode ser um vazamento e, para evitá-lo, não vamos utilizá-la.

features = df.columns.to_list()
features = list(set(features) - set(to_remove))
features.sort()



# MODEL SELECTION
X_train, X_test, y_train, y_test = model_selection.train_test_split(df_train[features], 
                                                                    df_train[target], 
                                                                    train_size=0.8, 
                                                                    random_state=42)
print('Proporção resposta treino:', y_train.mean())
print('Proporção resposta teste:', y_test.mean())



# EXPLORE
# Verifica variáveis com missing
X_train.isna().sum().sort_values(ascending=False)

# Flags de imputação
# Essas features receberão -1 nos missing
missing_minus_100 = ['avgIntervaloVendas',
                   'maxVolumeProduto',
                   'minVolumeProduto',
                   'avgVolumeProduto'
                   ]

# Essas features receberão 0 nos missing
missing_0 = ['avgQtdeParcelas', 
             'maxQtdeParcelas', 
             'minQtdeParcelas',
             'pctPedidoAtraso'
             ]



# TRANSFORM
imputer_minus_100 = imputation.ArbitraryNumberImputer(arbitrary_number=-100, variables=missing_minus_100)
imputer_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0, variables=missing_0)

# MODEL (já com os melhores parâmetros do gridsearch)
model = lightgbm.LGBMClassifier(n_jobs=-1,
                                learning_rate=0.1,
                                min_child_samples=30,
                                max_depth=10,
                                n_estimators=400)

# GRIDSEARCH
# params = {
#     "learning_rate": [0.1, 0.5, 0.7, 0.9, 0.99999],
#     "n_estimators":[300,400,450, 500],
#     "min_child_samples": [20,30,40,50,100]
# }

# grid = model_selection.GridSearchCV(model, params, cv=3, verbose=3, scoring='roc_auc')

# PIPELINE
model_pipeline = pipeline.Pipeline([('Imputer -100', imputer_minus_100),
                                    ('Imputer 0', imputer_0),
                                    # ("Grid search", grid),
                                    ('LightGBM model', model),
                                    ]) 

# TREINO DO ALGORITMO
model_pipeline.fit(X_train, y_train)


# METRICS
auc_train = metrics.roc_auc_score(y_train, model_pipeline.predict_proba(X_train)[:,1])
auc_test = metrics.roc_auc_score(y_test, model_pipeline.predict_proba(X_test)[:,1])
auc_oot = metrics.roc_auc_score(df_oot[target], model_pipeline.predict_proba(df_oot[features])[:,1])

metrics_model = {
    'auc_train': auc_train,
    'auc_test': auc_test,
    'auc_oot': auc_oot
    }

print(metrics_model)



# SALVANDO O MODELO
model_dict = {
    'model': model_pipeline,
    'features': X_train.columns.tolist()
}
model_dict.update(metrics_model)

model_results = pd.Series(model_dict)
model_results.to_pickle(f'{MODEL_DIR}/churn_olist_lgbm.pkl')
