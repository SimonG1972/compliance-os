import sqlite3

con = sqlite3.connect("compliance.db")
c = con.cursor()

def cnt(pat):
    return c.execute("SELECT COUNT(*) FROM documents WHERE url LIKE ?", (pat,)).fetchone()[0]

print("YT total docs:", cnt("https://www.youtube.com/%"))
print("YT /t/terms:", cnt("https://www.youtube.com/t/terms%"))
print("YT terms w/ hl=", cnt("https://www.youtube.com/t/terms%hl=%"))
print("YT terms w/ override_hl=", cnt("https://www.youtube.com/t/terms%override_hl=%"))
print("policies.google.com:", cnt("https://policies.google.com/%"))

con.close()
