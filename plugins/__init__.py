from airflow.plugins_manager import AirflowPlugin

import operators
import helpers


# Defining the plugin class
class SparkifyPlugin(AirflowPlugin):
    name = "sparkify_plugin"
    operators = [
        operators.LoadDimensionOperator,
        operators.LoadFactOperator,
        operators.StageTablesToRedshiftOperator,
        operators.DataQualityOperator,
        operators.CreateTableOperator
    ]

    helpers = [
        helpers.SqlQueries
    ]