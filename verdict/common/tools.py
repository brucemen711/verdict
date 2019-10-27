import pandas as pd
from .logging import *


def auto_str(cls):
    """
    https://stackoverflow.com/questions/32910096/is-there-a-way-to-auto-generate-a-str-implementation-in-python
    """
    def __str__(self):
        return '%s(%s)' % (
            type(self).__name__,
            ', '.join('%s=%s' % item for item in vars(self).items())
        )
    cls.__str__ = __str__
    return cls

def pandas_df_from_result(result, desc):
    col_types = [d[1] for d in desc]
    df = pd.DataFrame(result, columns=[d[0] for d in desc])
    for i, col_type in enumerate(col_types):
        if col_type == 'date':
            df[df.columns[i]] = pd.to_datetime(df[df.columns[i]])
    return df

def assert_array_equal(left, right):
    if (len(left) == len(right)) and (left == right).all():
        return
    else:
        msg = f'{left} (of type {type(left)}) and {right} (of type {type(right)}) are not equal.'
        log(msg, 'ERROR')
        raise ValueError(msg)

def assert_equal(left, right):
    if left == right:
        return
    else:
        msg = f'{left} (of type {type(left)}) and {right} (of type {type(right)}) are not equal.'
        log(msg, 'ERROR')
        raise ValueError(msg)

def assert_type(obj, expected_type):
    if isinstance(obj, expected_type):
        return
    else:
        msg = f'{obj} (of type {type(obj)}) is not an instnace of {expected_type}.'
        log(msg, 'ERROR')
        raise ValueError(msg)

def flatten(nested_list):
    # https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-list-of-lists
    return [item for sublist in nested_list for item in sublist]
