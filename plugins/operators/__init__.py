from operators.create_tables import CreateTableOperator
from operators.staging_redshift_tables   import StageTablesToRedshiftOperator
from operators.data_quality import DataQualityOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator


operators = [
    "CreateTableOperator",
    "StageTablesToRedshiftOperator",
    "LoadDimensionOperator",
    "LoadFactOperator",
    "DataQualityOperator"
]
