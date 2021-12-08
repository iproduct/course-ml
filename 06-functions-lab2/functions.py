books = [
    {"id": 1, "title": "Learning Python", "subtitle": "", "authors": ["Марк Лътз" "Дейвид Асър"],
     "publisher": "O'Reily", "year": 1999, "price": 22.7},
    {"id": 2, "title": "Think Python", "subtitle": "An Introduction to Software Design", "authors": ["Алън Б. Дауни"],
     "publisher": "O'Reily", "year": 2002, "price": 9.4},
    {"id": 3, "title": "Python Cookbook", "subtitle": "Recipes for Mastering Python 3",
     "authors": ["Браян К. Джоунс", "Дейвид М. Баазли"], "publisher": "O'Reily", "year": 2011, "price": 135.9}
]

if __name__ == "__main__":
    for b in books:
        print(
            f"| {b['id']:^3d} | {b['title']:<20.20s} | {b['subtitle']:<20.20s} | {', '.join(b['authors']):^25.25s} |"
            f" {b['publisher']:^10.10s} | {b['year']:<4d} | {b['price']:>7.2f} |")
