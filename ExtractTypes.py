"""
Enums that define the posibile criteria for creating an extract type
Things like delimiter, split by size, have clearly defined values that they are allowed to have
"""

class DelimiterEnum(Enum):
    VERTICAL_BAR = "vertical bar sep"
    TAB = "tab sep"
    COMMA = "comma sep"
    PARQUET = "parquet"


class FullyQualifiedEnum(Enum):
    NO = ""
    COMMA = '"'


class SplitBySizeEnum(Enum):
    SIZE_250MB = "250MB"
    ONE_FILE = "One file"
    FILES_500 = "500 files"
    SIZE_3GB = "3GB"
    SIZE_4GB = "4GB"


class StorageFilesEnum(Enum):
    YES = "yes"
    NO = "no"


class ArchiveType(Enum):
    NO = "no"
    ZIP = "zip"
    GZ = "gz"


class ExtensionEnum(Enum):
    TSV = "tsv"
    PSV = "psv"
    CSV = "csv"
    SNAPPY_PARQUET = "snappy.parquet"


"""
    This enum contains the paths to helper configuration scripts that are used in the class
"""
class ConfigurationEnum(Enum):
    PRODUCT_BASE = "extract-types"
    ERROR_HANDLING = "s3://bigdbm-scripts/common/errorhandling-manager.py"
    GENERAL_CHECKS = "s3://bigdbm-scripts/common/general-checks.py"
    GENERAL_STANDARDIZATIONS = "s3://bigdbm-scripts/common/general-standardizations.py"
    FILE_READ_MANAGER = "s3://bigdbm-scripts/common/file-read-manager.py"
    FILE_WRITE_MANAGER = "s3://bigdbm-scripts/common/file-write-manager.py"
    CONFIGURATION_MANAGER = "s3://bigdbm-scripts/configurations/configuration-manager.py"
    CONFIGURATION_LAYOUT_PATH = "s3://bigdbm-configuration/layouts/extracts/"
    """
    This value represents a mapper that maps the name of a parameter to its assigned Enum
    This is useful in the class definition below
    """
    ENUM_MAP = {
        "delimiter": DelimiterEnum,
        "fully_qualified": FullyQualifiedEnum,
        "split_by_size": SplitBySizeEnum,
        "storage_files": StorageFilesEnum,
        "archive_type": ArchiveType,
        "extension": ExtensionEnum
    }

from enum import Enum
import importlib
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import uuid
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

spark.sparkContext.addFile(path=ConfigurationEnum.ERROR_HANDLING.value)
spark.sparkContext.addFile(path=ConfigurationEnum.GENERAL_CHECKS.value)
spark.sparkContext.addFile(path=ConfigurationEnum.GENERAL_STANDARDIZATIONS.value)
spark.sparkContext.addFile(path=ConfigurationEnum.FILE_READ_MANAGER.value)
spark.sparkContext.addFile(path=ConfigurationEnum.FILE_WRITE_MANAGER.value)
spark.sparkContext.addFile(path=ConfigurationEnum.CONFIGURATION_MANAGER.value)
spark.sparkContext.addFile(path = ConfigurationEnum.GENERAL_CHECKS.value)

ConfigurationUDW = getattr(importlib.import_module("configuration-manager"), "ConfigurationUDW")
WriteFolderParquet = getattr(importlib.import_module("file-write-manager"), "WriteFolderParquet")
FolderReaderManager = getattr(importlib.import_module("file-read-manager"), "ReadFolderParquet")

general_checks = importlib.import_module('general-checks')
general_standardizations = importlib.import_module('general-standardizations')
add_slash_to_path = getattr(general_standardizations, 'add_slash_to_path')


class ExtractTypes:
    def __init__(self):
        """
        Path where the extract types are stored
        Currently it's => s3://bigdbm-processing/work-luca/extract-types/
        """
        self.__extractTypesS3Path = self.__getExtractTypesS3Path()

    @staticmethod
    def __getExtractTypesS3Path():
        """
        Use the ConfigurationUDW class to get the required path for extract types

        This path will be updated when the script will be moved to prod
        """
        configuration_udw = ConfigurationUDW(spark=spark)
        return configuration_udw.general_get_udw_productpath(productbase=ConfigurationEnum.PRODUCT_BASE.value)

    def addExtractType(
            self,
            layout_id=None,
            delimiter=None,
            fully_qualified=None,
            split_by_size=None,
            storage_files=None,
            archive_type=None,
            extension=None,
            internal_name="",
            naming_convention="",
            example="",
            observation=""
    ):
        """
        Adding a new extract type
        Keyword parameters with default values assigned to None are mandatory when creating a new extract
        while those assigned as an empty string are not mandatory

        With this logic, an extract type MUST have a layout_id, delimiter etc. while it's not neccesary for a user to
        define an example, observation etc.

        """

        extract_type_uid = self.__generate_salted_uuid()

        """
        Get a dictionary of ALL the parameters, excluding the self parameter
        These will be validated and then the dataframe of the new extract type will be created with them
        """
        params = locals()
        params.pop("self")

        """
        Validate the mandatory parameters 
        This method will throw an error if any parameters are not valid
        """
        self.__validateForAdd(**params)

        """
        After validation,
        Generate a random uuid using uuid4 + a timestamp with 6 decimal points
        and add it to the params dictionary that will be used to create the extract type dataframe 
        """
        extract_type_uid = self.__generate_salted_uuid()
        params["extract_type_uid"] = extract_type_uid
        extract_type_df = spark.createDataFrame([params])

        """
        Perform a check to see if the same exact extract type has already been created
        """
        self.__extractTypeAlreadyExists(extract_type_df)

        """
        If it's a new extract type, use the WriteFolderParquet to write the new extract type to s3
        """
        new_extract_type_path = self.__extractTypesS3Path + f'/{extract_type_uid}'
        extract_type_df.show(1, vertical=True, truncate=False)
        WriteFolderParquet().write(spark=spark, \
                                   source_df=extract_type_df, \
                                   file_path=new_extract_type_path \
                                   )

    def getExtractType(
            self,
            layout_id=None,
            delimiter=None,
            fully_qualified=None,
            split_by_size=None,
            storage_files=None,
            archive_type=None,
            extension=None,
            internal_name=None,
            naming_convention=None,
            example=None,
            observation=None,
    ):
        """
        This method will return a dataframe with all extract types found with the given input parameters

        For this method, no parameters are mandatory, extract types can be filtered by 0 or more criteria
        """

        """
        Extract all the parameters that are not None => parameters that have been set as filtering criteria by the user
        """
        params = locals()
        params.pop('self')
        non_empty_params = {
            k: v for k, v in params.items() if v is not None
        }

        self.__validateForGet(**non_empty_params)

        """
        Read extract types using the FolderReaderManager class 
        Dynamically filter the dataframe using the non_empty_params dictionary
        """
        extract_types = FolderReaderManager().read(spark=spark, file_path=self.__extractTypesS3Path)

        for k, v in non_empty_params.items():
            extract_types = extract_types.filter(col(k) == v)

        return extract_types

    @staticmethod
    def __checkIfLayoutExists(layout_id):
        """
        Use the ConfigurationEnum to get the base layout path to which we add the layout id parameter
        we then attempt to read from that path
        If this path doesn't exist, an error will be thrown
        """
        layout_path = ConfigurationEnum.CONFIGURATION_LAYOUT_PATH.value + f'extractlayoutid={layout_id}/'
        FolderReaderManager().read(spark=spark, file_path=layout_path)

    @staticmethod
    def __generate_salted_uuid():
        """
        Generate a salted uid using uuid4 with a timestamp

        Concurrency problems using this combination is astronomically low
        """
        time_salt = f"{datetime.now().timestamp():.6f}"
        raw_uuid = str(uuid.uuid4())
        return f"{raw_uuid}_{time_salt}"

    """
    2 methods for input validation, 1 for adding an extract and 1 for getting extracts


    The reasaon why there are 2 separate functions is that the logic for the layout_id argument is different in the case of each function


    For addExtract, layout_id is mandatory, hence why in this method __validateForAdd, an error is thrown if the argument is not present
    However in __validateForGet, layout_id is not mandatory (since we can filter by any attributes we want) so we don't throw an error if it's not present


    Next, both functions validate that the layout_id is a valid one and convert it to string
    We convert it because otherwise we could have the following problem 
        extract_type1 -> layout_id=1 (int)
        extract_type2 -> layout_id=1 (string)


    Both functions call the __validateInputParameters that performs the actual validation
    """

    def __validateForAdd(self, **kwargs):
        if "layout_id" not in kwargs:
            raise ValueError("layout_id is required for addExtractType.")

        self.__checkIfLayoutExists(kwargs["layout_id"])
        kwargs["layout_id"] = str(kwargs["layout_id"])
        self.__validateInputParameters(**kwargs)

    def __validateForGet(self, **kwargs):
        if "layout_id" in kwargs:
            self.__checkIfLayoutExists(kwargs["layout_id"])
            kwargs["layout_id"] = str(kwargs["layout_id"])

        self.__validateInputParameters(**kwargs)

    def __validateInputParameters(self, **kwargs):
        """
        This function is a validator of input paramters given by the user in the addExtractType and getExtractType methods


        Retreieve the enum_map from configuration for easier and cleaner validation of the parameters
        It maps the name of a keyword argument to its assigned Enum
        e.g. delimiter -> DelimiterEnum
        """
        enum_map = ConfigurationEnum.ENUM_MAP.value

        """
        Loop through all paramters given for validation
        """
        for param_name, value in kwargs.items():
            if value is None:
                raise ValueError(f"{param_name} cannot be null")

            """
            e.g. For param_name = delimiter, its enum_class = DelimiterEnum using the enum_map we defined earlier

            This way, we check if the given value for delimiter is a valid one by checking if it is in the values of its enum

            e.g. delimiter='vertical bar sep' => valid, is present as a value in the enum
                delimiter= 'tab' => not valid
            """
            if param_name in enum_map:
                enum_class = enum_map[param_name]
                valid_values = [e.value for e in enum_class]
                if value not in valid_values:
                    raise ValueError(
                        f"{param_name} value is invalid: {value}. "
                        f"Values are: {', '.join(valid_values)}"
                    )

    def __extractTypeAlreadyExists(self, new_extract_type_df):
        """
        Method that checks if the input parameter dataframe has already been created
        """

        """
        Get all existing extract types and compare them against the new dataframe
        This is wrapped in a try block since if the extract_type folder is empty, the read will throw an error
        """
        try:
            all_extract_types = FolderReaderManager().read(spark=spark, file_path=self.__extractTypesS3Path)
        except Exception:
            print("No existing extract types found. Skipping duplicate check.")
            return

        """
        To check if the dataframe existis, we must compare all columns except for the uid so we drop that column from both dataframes
        """
        existing = all_extract_types.drop("extract_type_uid")
        new = new_extract_type_df.drop("extract_type_uid")

        match = existing.intersect(new)

        """
        If an exact match has been found, throw an error 
        """
        if match.count() > 0:
            raise ValueError("This extract already exists!")
