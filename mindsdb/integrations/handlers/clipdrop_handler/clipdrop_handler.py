from typing import Optional, Dict
import pandas as pd

from mindsdb.integrations.handlers.clipdrop_handler.clipdrop import ClipdropClient

from mindsdb.integrations.libs.base import BaseMLEngine

from mindsdb.utilities.log import get_log


logger = get_log("integrations.clipdrop_handler")


class ClipdropHandler(BaseMLEngine):
    name = "clipdrop"

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        args = args['using']

        available_tasks = ["remove_text", "remove_background", "sketch_to_image", "text_to_image", "replace_background", "reimagine"]

        if 'api_key' not in args:
            raise Exception('api_key has to be specified')

        if 'task' not in args:
            raise Exception('task has to be specified. Available tasks are - ' + available_tasks)

        if args['task'] not in available_tasks:
            raise Exception('Unknown task specified. Available tasks are - ' + available_tasks)

        if 'local_directory_path' not in args:
            raise Exception('local_directory_path has to be specified')

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if 'using' not in args:
            raise Exception("Clipdrop AI Inference engine requires a USING clause! Refer to its documentation for more details.")
        self.generative = True

        args = args['using']
        args['target'] = target
        self.model_storage.json_set('args', args)

    def _get_clipdrop_client(self, args):
        api_key = self._get_clipdrop_api_key(args)

        local_directory_path = args["local_directory_path"]

        return ClipdropClient(api_key=api_key, local_dir=local_directory_path)

    def _process_remove_text(self, df, args):

        def generate_remove_text(conds, client):
            conds = conds.to_dict()
            return client.remove_text(conds.get("image_url"))

        supported_params = {"image_url"}

        if "image_url" not in df.columns:
            raise Exception("`image_url` column has to be given in the query.")

        for col in df.columns:
            if col not in supported_params:
                raise Exception(f"Unknown column {col}. Currently supported parameters for remove text - {supported_params}")

        client = self._get_clipdrop_client(args)

        return df[df.columns.intersection(supported_params)].apply(generate_remove_text, client=client, axis=1)

    def _process_remove_background(self, df, args):

        def generate_remove_background(conds, client):
            conds = conds.to_dict()
            return client.remove_background(conds.get("image_url"))

        supported_params = {"image_url"}

        if "image_url" not in df.columns:
            raise Exception("`image_url` column has to be given in the query.")

        for col in df.columns:
            if col not in supported_params:
                raise Exception(f"Unknown column {col}. Currently supported parameters for remove background - {supported_params}")

        client = self._get_clipdrop_client(args)

        return df[df.columns.intersection(supported_params)].apply(generate_remove_background, client=client, axis=1)

    def _process_sketch_to_image(self, df, args):

        def generate_sketch_to_image(conds, client):
            conds = conds.to_dict()
            return client.sketch_to_image(conds.get("image_url"), conds.get("text"))

        supported_params = {"image_url", "text"}

        if "image_url" not in df.columns:
            raise Exception("`image_url` column has to be given in the query.")

        if "text" not in df.columns:
            raise Exception("`text` column has to be given in the query.")

        for col in df.columns:
            if col not in supported_params:
                raise Exception(f"Unknown column {col}. Currently supported parameters for remove background - {supported_params}")

        client = self._get_clipdrop_client(args)

        return df[df.columns.intersection(supported_params)].apply(generate_sketch_to_image, client=client, axis=1)

    def _process_text_to_image(self, df, args):

        def generate_text_to_image(conds, client):
            conds = conds.to_dict()
            return client.text_to_image(conds.get("text"))

        supported_params = {"text"}

        if "text" not in df.columns:
            raise Exception("`text` column has to be given in the query.")

        for col in df.columns:
            if col not in supported_params:
                raise Exception(f"Unknown column {col}. Currently supported parameters for remove background - {supported_params}")

        client = self._get_clipdrop_client(args)

        return df[df.columns.intersection(supported_params)].apply(generate_text_to_image, client=client, axis=1)

    def _process_replace_background(self, df, args):

        def generate_replace_background(conds, client):
            conds = conds.to_dict()
            return client.replace_background(conds.get("image_url"), conds.get("text"))

        supported_params = {"image_url", "text"}

        if "image_url" not in df.columns:
            raise Exception("`image_url` column has to be given in the query.")

        if "text" not in df.columns:
            raise Exception("`text` column has to be given in the query.")

        for col in df.columns:
            if col not in supported_params:
                raise Exception(f"Unknown column {col}. Currently supported parameters for replace background - {supported_params}")

        client = self._get_clipdrop_client(args)

        return df[df.columns.intersection(supported_params)].apply(generate_replace_background, client=client, axis=1)

    def _process_reimagine(self, df, args):

        def generate_reimagine(conds, client):
            conds = conds.to_dict()
            return client.reimagine(conds.get("text"))

        supported_params = {"text"}

        if "text" not in df.columns:
            raise Exception("`text` column has to be given in the query.")

        for col in df.columns:
            if col not in supported_params:
                raise Exception(f"Unknown column {col}. Currently supported parameters for reimagine - {supported_params}")

        client = self._get_clipdrop_client(args)

        return df[df.columns.intersection(supported_params)].apply(generate_reimagine, client=client, axis=1)

    def predict(self, df, args=None):

        args = self.model_storage.json_get('args')

        if args["task"] == "remove_text":
            preds = self._process_remove_text(df, args)
        elif args["task"] == "remove_background":
            preds = self._process_remove_background(df, args)
        elif args["task"] == "sketch_to_image":
            preds = self._process_sketch_to_image(df, args)
        elif args["task"] == "text_to_image":
            preds = self._process_text_to_image(df, args)
        elif args["task"] == "replace_background":
            preds = self._process_replace_background(df, args)
        elif args["task"] == "reimagine":
            preds = self._process_reimagine(df, args)

        result_df = pd.DataFrame()

        result_df['predictions'] = preds

        result_df = result_df.rename(columns={'predictions': args['target']})

        return result_df

    def _get_clipdrop_api_key(self, args):
        if 'api_key' in args:
            return args['api_key']

        connection_args = self.engine_storage.get_connection_args()

        if 'api_key' in connection_args:
            return connection_args['api_key']

        raise Exception("Missing API key 'api_key'. Either re-create this ML_ENGINE specifying the `api_key` parameter\
                 or re-create this model and pass the API key with `USING` syntax.")
