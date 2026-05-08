# snowflakeBI

Repo for CAI BI Snowflake account

## Requirements

- Python 3.8 or higher
- dbt-core
- dbt-snowflake
- snowflake-connector-python

## Usage

1. Clone the repository

    ```bash
        $git clone git@github.com:DataVoke/snowflakeBI.git
        $cd snowflakeBI
    ```

2. Create a virtual environment and activate it:

    ```bash
        $python -m venv <venv_name>
        $source <venv_name>/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3. Ensure that Python's package installer, `pip`, is up to date:

    ```bash
        $pip install --upgrade pip
    ```

4. Install the required packages using `pip install -r requirements.txt`
5. Set up your dbt connection profile in `~/.dbt/profiles.yml` with your Snowflake account details (see the example configuration below)
6. Run the dbt commands to build and test your models (NOTE: Make sure to run these commands from the root directory of the **project** where the `dbt_project.yml` file is located)
7. (Optional) Deactivate the virtual environment when you're done:

    ```bash
        $deactivate
    ```

### Typical dbt commands

#### Run models    
`dbt run` to execute the models
    
`dbt run --models <model_name>` to run a specific model

`dbt run --select <model_name>` to select specific models to run

`dbt run --select <model_name> --debug` to run a model with debug information

#### Test models
`dbt test` to run tests on your models

`dbt test --models <model_name>` to test a specific model

`dbt test --select <model_name>` to select specific models to test

`dbt test --select <model_name> --debug` to test a model with debug information

#### Docs        
`dbt docs generate` to create documentation for the models

`dbt docs serve` to serve the documentation locally

## Template for ~/.dbt/profiles.yml

Make sure to set up your `profiles.yml` file with the correct Snowflake connection details. Here's an example configuration:

```yaml
cai_bi:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <CAI_SNOWFLAKE_ACCOUNT>
      user: <USERNAME(EMAIL)>
      # SSO config
      authenticator: externalbrowser
      database: <DATABASE_NAME>
      role: <ROLE_NAME>
      warehouse: <WAREHOUSE_NAME>
      schema: <SCHEMA_NAME>
      threads: 4
```

Replace the placeholders with your actual Snowflake account details. This configuration allows dbt to connect to your Snowflake data warehouse and execute the models defined in your project.
