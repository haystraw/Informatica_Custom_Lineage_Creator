def generate_transitions(input_string):
    """
    Takes a string with elements separated by '/' and returns a string
    showing the transitions between consecutive elements.

    Parameters:
    input_string (str): Input string with elements separated by '/'.

    Returns:
    str: A string with transitions in the format 'atob, btoc, ...'.
    """
    elements = input_string.split('/')  # Split the input string by '/'
    transitions = [f"{elements[i]}to{elements[i+1]}" for i in range(len(elements) - 1)]
    return ", ".join(transitions)

# Example usage
input_string = "a/b/c/d/e"
result = generate_transitions(input_string)
print(result)
