from unittest import TestCase
from unittest.mock import patch, MagicMock
import pandas as pd
import io
patch("azure.storage.filedatalake.DataLakeServiceClient", MagicMock()).start()



from utils.dataframes import convert_to_in_memory_parquet, get_dataframe, save_dataframe, update_dataframe

class TestDataFrames(TestCase):

    def test_convert_to_in_memory_parquet(self):
        df = pd.DataFrame()

        binary = convert_to_in_memory_parquet(df)

        self.assertIsInstance(binary, io.BytesIO, "result is not an instance of io.BytesIO")


    @patch('utils.dataframes.get_file_from_datalake')
    def test_get_dataframe_file_found(self, mock_get_file_from_datalake):
        parquet = io.BytesIO()
        mock_df_parquet = pd.DataFrame({'close': [1]})
        mock_df_parquet.to_parquet(parquet, index=False)
        parquet.seek(0)
        mock_get_file_from_datalake.return_value = parquet.read()

        returned_dataframe = get_dataframe('file_name', 'file_path')

        self.assertIsInstance(returned_dataframe, pd.DataFrame)
        mock_get_file_from_datalake.assert_called_once_with('file_name', 'file_path')
        pd.testing.assert_frame_equal(mock_df_parquet, returned_dataframe)

    @patch('utils.dataframes.get_file_from_datalake')
    def test_get_dataframe_file_not_found(self, mock_get_file_from_datalake):
        mock_get_file_from_datalake.return_value = None
        returned_dataframe = get_dataframe('file_name', 'file_path')

        self.assertIsInstance(returned_dataframe, pd.DataFrame)
        self.assertEqual(len(returned_dataframe), 0)
        mock_get_file_from_datalake.assert_called_once_with('file_name', 'file_path')

    @patch('utils.dataframes.send_file_to_datalake')
    @patch('utils.dataframes.convert_to_in_memory_parquet')
    def test_save_dataframe_with_file_path(self, mock_convert_to_in_memory_parquet, mock_send_file_to_datalake):
        mock_parquet_file = io.BytesIO()
        mock_convert_to_in_memory_parquet.return_value = mock_parquet_file

        save_dataframe(mock_parquet_file, 'file_name', 'file_path')

        mock_send_file_to_datalake.assert_called_once_with(
            mock_parquet_file,
            'file_name',
            'file_path'
        )

    @patch('utils.dataframes.send_file_to_datalake')
    @patch('utils.dataframes.convert_to_in_memory_parquet')
    def test_save_dataframe_without_file_path(self, mock_convert_to_in_memory_parquet, mock_send_file_to_datalake):
        mock_parquet_file = io.BytesIO()
        mock_convert_to_in_memory_parquet.return_value = mock_parquet_file

        save_dataframe(mock_parquet_file, 'file_name')

        mock_send_file_to_datalake.assert_called_once_with(
            mock_parquet_file,
            'file_name',
            None
        )


    def test_update_dataframe_edit_existing_row(self):
        df_to_update = pd.DataFrame({"id": [1,2], "close": [0,0]})
        df_updates = pd.DataFrame({"id": [1], "close": [3]})
        expected_df = pd.DataFrame({"id": [1,2], "close": [3,0]}).sort_values(by='id').reset_index(drop=True)

        updated_df = update_dataframe(df_to_update, df_updates, "id")

        updated_df.sort_values(by='id', inplace=True)
        updated_df.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(
            updated_df,
            expected_df
        )

    def test_update_dataframe_new_update(self):
        df_to_update = pd.DataFrame({"id": [1,2], "close": [0,0]})
        df_updates = pd.DataFrame({"id": [3], "close": [3]})
        expected_df = pd.DataFrame({"id": [1,2,3], "close": [0,0,3]}).sort_values(by='id').reset_index(drop=True)

        updated_df = update_dataframe(df_to_update, df_updates, "id")
        updated_df.sort_values(by='id', inplace=True)
        updated_df.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(
            updated_df,
            expected_df
        )

