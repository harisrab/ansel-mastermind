def balance_elements(data_dict):
    """
    Balance the number of elements in each list in a dictionary.

    This function iterates over each key-value pair in the dictionary. If the value is a list,
    it appends Nones to the list until its length equals the maximum list length found in the
    dictionary. If the value is not a list, it is left unchanged.

    Args:
    data_dict (dict): The dictionary whose lists are to be balanced.

    Returns:
    dict: A dictionary with balanced lists.
    """

    values = data_dict.values()

    num_lists = sum(1 for v in values if isinstance(v, list))

    # If there are no lists in the dictionary values, just return the original dictionary
    if num_lists == 0:
        return data_dict

    # Get the max length among all value lists in the dictionary
    max_length = max(len(v) for v in values if isinstance(v, list))

    # Make all lists in the dictionary the same length as max_length
    for key, value in data_dict.items():
        if isinstance(value, list):
            diff = max_length - len(value)
            if diff > 0:
                data_dict[key].extend([None]*diff)

    return data_dict
