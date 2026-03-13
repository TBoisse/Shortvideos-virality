# extern
import time
log_flag = False
debug_flag = False
log_path = "PrintUtils/log.txt"

color_to_type = {
    "\033[92m" : "Info...> ",
    "\033[93m" : "Warning> ",
    "\033[91m" : "Error..> ",
    "\033[94m" : "Debug..> "
}

def get_current_time() -> str:
    """
    ### Compute current time in the day as hh:mm:ss.

    Returns:
        (str) : The current time of the day.
    """
    _time = time.localtime()
    hour = "0" + str(_time.tm_hour)
    min = "0" + str(_time.tm_min)
    sec = "0" + str(_time.tm_sec)
    return f"{hour[-2:]}:{min[-2:]}:{sec[-2:]}"

def log(message : str, color : str, to_return : bool = False):
    """
    ### Print a message preceding by the current time of the day in a certain color and logs it into a file.

    Args:
        message (str) : The message to print.
        color (str) : The color to use. (terminal color eg : \033[92m for green)
        to_return (bool) : If true, return the colored message instead of printing it.
    """
    colored_message = color + get_current_time() + " || " + str(message) + "\033[0m"
    if log_flag: 
        with open(log_path, "a") as f:
            f.write(color_to_type[color] + get_current_time() + " || " + str(message) + "\n")
    if to_return:
        return colored_message
    return print(colored_message)

def logInfo(message : str, to_return : bool = False):
    """
    ### Print a message preceding by the current time of the day in green.

    Args:
        message (str) : The message to print.
        to_return (bool) : If true, return the colored message instead of printing it.
    """
    return log(message, "\033[92m", to_return)

def logWarning(message : str, to_return : bool = False):
    """
    ### Print a message preceding by the current time of the day in orange/yellow.

    Args:
        message (str) : The message to print.
        to_return (bool) : If true, return the colored message instead of printing it.
    """
    return log(message, "\033[93m", to_return)
    
def logError(message : str, to_return : bool = False):
    """
    ### Print a message preceding by the current time of the day in red.

    Args:
        message (str) : The message to print.
        to_return (bool) : If true, return the colored message instead of printing it.
    """
    return log(message, "\033[91m", to_return)

def logDebug(message : str, debug : bool = False, to_return : bool = False):
    """
    ### Print a message preceding by the current time of the day in blue if the debug parameter is True.

    Args:
        message (str) : The message to print.
        debug (bool) : Flag to be True if we want to print this message.
        to_return (bool) : If true, return the colored message instead of printing it.
    """
    if debug_flag or debug:
        return log(message, "\033[94m", to_return)