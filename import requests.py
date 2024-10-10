import requests
from bs4 import BeautifulSoup

def fetch_and_parse_google_doc(url):
    """
    Fetch the content of the Google Doc and return parsed character data.
    Each entry contains a character and its (x, y) coordinates.
    """
    response = requests.get(url)
    
    if response.status_code != 200:
        print("Failed to retrieve the document.")
        return None
    
    # Parse the HTML content
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extract the document text (you may need to adjust based on document structure)
    doc_content = soup.find_all('div', {'class': 'kix-page-content'})
    
    # Initialize an empty list to hold character data
    character_data = []

    # Iterate over the content to extract character, x, y
    for page in doc_content:
        paragraphs = page.find_all('p')
        for para in paragraphs:
            para_text = para.get_text(separator=" ", strip=True)
            # Look for lines specifying a character, x, and y coordinate
            if "Character:" in para_text and "X:" in para_text and "Y:" in para_text:
                # Parse the line (example: "Character: A, X: 0, Y: 0")
                parts = para_text.split(',')
                char = parts[0].split('Character:')[-1].strip()
                x_coord = int(parts[1].split('X:')[-1].strip())
                y_coord = int(parts[2].split('Y:')[-1].strip())
                # Append parsed data to the list
                character_data.append((char, x_coord, y_coord))
    
    return character_data

def build_grid(character_data):
    """
    Build a 2D grid based on the character data and return the grid.
    Any unspecified positions in the grid will be filled with a space (' ').
    """
    if not character_data:
        return
    
    # Find the max x and y to determine the grid size
    max_x = max([x for _, x, _ in character_data])
    max_y = max([y for _, _, y in character_data])

    # Initialize the grid with spaces
    grid = [[' ' for _ in range(max_x + 1)] for _ in range(max_y + 1)]

    # Place each character at its respective (x, y) position in the grid
    for char, x, y in character_data:
        grid[y][x] = char
    
    return grid

def print_grid(grid):
    """
    Print the grid row by row, so it forms a graphic in fixed-width font.
    """
    if not grid:
        return
    
    for row in grid:
        print(''.join(row))

def retrieve_and_print_google_doc_grid(url):
    """
    Main function that retrieves, parses, builds, and prints the character grid from the Google Doc.
    """
    # Step 1: Fetch and parse the document
    character_data = fetch_and_parse_google_doc(url)

    # Step 2: Build the grid
    grid = build_grid(character_data)

    # Step 3: Print the grid
    print_grid(grid)

# Example usage with the provided URL
url = "https://docs.google.com/document/d/e/2PACX-1vRMx5YQlZNa3ra8dYYxmv-QIQ3YJe8tbI3kqcuC7lQiZm-CSEznKfN_HYNSpoXcZIV3Y_O3YoUB1ecq/pub"
retrieve_and_print_google_doc_grid(url)
