import json
from collections import Counter

path = "subastas_detalle.jsonl"

counter = Counter()
total = 0
errores = 0

with open(path, "r", encoding="utf-8") as f:
    for num_linea, line in enumerate(f, start=1):
        line = line.strip()
        if not line:
            continue

        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            errores += 1
            print(f"JSON inválido en línea {num_linea}")
            continue

        total += 1
        provincia = data.get("meta_provincia")
        counter[provincia] += 1

print(f"Total registros: {total}")
print(f"Líneas con error JSON: {errores}")
print()

for provincia, cantidad in sorted(counter.items(), key=lambda x: str(x[0])):
    print(provincia, cantidad)

print()
print("Valencia:", counter.get("46", 0))