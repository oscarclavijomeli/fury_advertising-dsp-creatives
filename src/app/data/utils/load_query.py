"Read and format query with parameters"


def load_format(path: str, params: dict = None) -> str:
    """
    Load a query from path and format it using parameters

    :param str path: Path where the query to be executed is located
    :param dict params: Values of the parameters included in the query to be executed

    :return: Query formated with the values of its parameters
    :rtype: str
    """

    if params is None:
        params = {}

    with open(path, "r", encoding="utf-8") as file:
        query = file.read()

    return query.format(**params)
