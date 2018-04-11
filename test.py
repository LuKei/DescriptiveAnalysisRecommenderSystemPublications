from fuzzywuzzy import fuzz

def match_author(author_a, author_b, threshold=0.95):
    return True if fuzz.token_sort_ratio(author_a, author_b) > threshold else False

print(match_author('James Caveree', 'Negar Haririr'))
