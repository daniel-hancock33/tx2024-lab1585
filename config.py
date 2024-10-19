# import libraries
import datetime
import nzpy
from IPython.core.display import HTML, display

def apply_table_styling():
    # Define CSS styling for tables
    table_css = 'table {align:left;display:block}'
    
    # Apply the CSS styling
    display(HTML('<style>{}</style>'.format(table_css)))

def connect_nzdb(nz_user, nz_password, nz_host, nz_database, nz_port):
    """
    Connect to a Netezza cloud instance and print connection details.

    Parameters:
    - nz_user (str): Username for the Netezza database
    - nz_password (str): Password for the Netezza database
    - nz_host (str): Hostname or IP address of the Netezza database
    - nz_database (str): Name of the Netezza database
    - nz_port (int): Port number for the Netezza database

    Returns:
    - nzcon: Connection object if successful, None otherwise
    """
    try:
        # Connect to Netezza cloud instance
        nzcon = nzpy.connect(user=nz_user, password=nz_password, host=nz_host, database=nz_database, port=int(nz_port))
        print(f"Connection to database {nz_database} successful.\n")
        return nzcon  # Return the connection object
    except nzpy.DatabaseError as e:
        # Handle database connection errors
        print(f"Failed to connect to the database: {nz_database}\n", str(e))
    except Exception as e:
        # Handle other potential errors
        print("Error:", str(e))

    # Print connection details if successful
    print(f"Host     : {nz_host}")
    print(f"Port     : {nz_port}")
    print(f"User     : {nz_user}")
    print(f"Password : ********")
    print(f"Password : {nz_password}")
    print(f"Database : {nz_database}")
    
    return None  # Return None if connection fails

# Function to disconnect from the database
def disconnect(nz_connection, nz_database):
    if nz_connection:
        try:
            nz_connection.close()
            print(f"Successfully disconnected from the database {nz_database}.")
        except AttributeError:
            nz_connection.close()
            print(f"Error: Failed to disconnect from the database {nz_database}.")

def run_nzsql(nz_connection, sql_command):
    """
    Executes a single SQL command using an existing Netezza database connection.

    Parameters:
    - conn: Existing database connection object.
    - sql_command: SQL command to be executed.
    """
    cursor = None
    
    print("--------Running SQL Command---------\n")
        
    try:
        # Create a cursor object using the existing connection
        cursor = nz_connection.cursor()
        #print("Cursor created successfully\n")

        # Execute the SQL command
        try:
            cursor.execute(sql_command)
            print(f"{sql_command}\nSuccessfully executed command.\n")
        except Exception as e:
            print(f"{sql_command}\nError executing command.")
            print(f"Error details: {e}")

        # Commit the transaction
        try:
            nz_connection.commit()
            #print("Transaction committed successfully.\n")
        except Exception as e:
            print("Error committing transaction")
            print(f"Error details: {e}")

    except Exception as e:
        print("Error setting up cursor or executing command")
        print(f"Error details: {e}")

    finally:
        # Ensure cursor is closed
        if cursor:
            try:
                cursor.close()
                #print("Cursor closed successfully.\n")
            except Exception as e:
                print("Error closing cursor")
                print(f"Error details: {e}")
    print  ("------------------------------------\n")

def verify_student_id(student_id, nz_host, nz_port, nz_user, nz_password, nz_sdb, nz_sschema):
    # Set the flag
    flag = False  # Set to False to disable prints
    
    # Check if student_id is "XX" or not within the range "01" to "30" and print a message
    if student_id == "XX":
        print("\nThe student ID is set to XX, please change to your assigned ID.")
        print("Return to the top of the notebook and rerun this cell.\n")
    elif not student_id.isdigit() or not (1 <= int(student_id) <= 30):
        print("\nInvalid student ID. Please enter a valid ID between 01 and 30.\nReturn to the top of the notebook to set your assigned student ID.\n")
    else:
        print(f"\nStudent ID       : {student_id}")
        flag = True
    
    if flag:
        # Print to verify
        print(f"Host             : {nz_host}")
        print(f"User             : {nz_user}") # get from args
        print(f"Password         : {nz_password}") # get from args
        print(f"Port             : {nz_port}") 
        print(f"Source Database  : {nz_sdb}")
        print(f"Source Schema    : {nz_sschema}")

def run_nzsql_withresults(nz_connection, sql_command):
    """
    Executes a single SQL command using an existing Netezza database connection and returns the results.

    Parameters:
    - nz_connection: Existing database connection object.
    - sql_command: SQL command to be executed.

    Returns:
    - Results of the SQL query if successful, else None.
    """
    cursor = None
    results = None
    
    print("--------Running SQL Command---------\n")
    
    try:
        # Create a cursor object using the existing connection
        cursor = nz_connection.cursor()

        # Execute the SQL command
        try:
            cursor.execute(sql_command)
            print(f"{sql_command}\nSuccessfully executed command.\n")
            
            # Fetch results if it is a SELECT statement
            if sql_command.strip().upper().startswith("SELECT"):
                results = cursor.fetchall()
                
        except Exception as e:
            print(f"{sql_command}\nError executing command.")
            print(f"Error details: {e}")

        # Commit the transaction if it is not a SELECT statement
        if not sql_command.strip().upper().startswith("SELECT"):
            try:
                nz_connection.commit()
                #print("Transaction committed successfully.\n")
            except Exception as e:
                print("Error committing transaction")
                print(f"Error details: {e}")

    except Exception as e:
        print("Error setting up cursor or executing command")
        print(f"Error details: {e}")

    finally:
        # Ensure cursor is closed
        if cursor:
            try:
                cursor.close()
                #print("Cursor closed successfully.\n")
            except Exception as e:
                print("Error closing cursor")
                print(f"Error details: {e}")
                
    print("--------End of SQL Command---------\n")

    return results
