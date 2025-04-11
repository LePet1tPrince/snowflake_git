import pytest
from unittest.mock import MagicMock
import pandas as pd
from procedure.manipulate_data  import double_the_table  # Replace 'your_module' with the actual module name

def test_double_the_table():
    # Mock the Snowflake session
    mock_session = MagicMock()

    # Create a mock DataFrame to simulate the table in Snowflake
    mock_df = pd.DataFrame({
        'ID': [1, 2],
        'NAME': ['Timmy', 'Vivian'],
        'DESCRIPTION': ['Hi, My name is Timmy', 'Wow, Vivian is amazing']
    })

    # Mock the session.table().to_pandas() call to return the mock DataFrame
    mock_session.table.return_value.to_pandas.return_value = mock_df

    # Call the function
    result = double_the_table(mock_session)

    # Assertions
    assert result == "SUCCESS"  # Ensure the function returns "SUCCESS"
    assert 'NEW_COLUMN' in mock_df.columns  # Ensure the new column is added
    assert all(mock_df['NEW_COLUMN'] == 'Making a change!!')  # Ensure the new column has the correct value

    # Verify that write_pandas was called with the correct arguments
    mock_session.write_pandas.assert_called_once_with(
        df=mock_df,
        schema="PUBLIC",
        table_name="NEW_GIT_TABLE",
        overwrite=True,
        auto_create_table=True
    )
