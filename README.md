# ipssi-moaa

Pour exporter son juptyer notebook en .py :

`jupytext --format-options notebook_metadata_filter="-all" --to py:percent <filename>.ipynb`

Pour exporter un fichier .py en juptyer notebook :

`jupytext --to ipynb <filename>.py`

Ne pas commiter les jupyter notebook. Commiter les .py.
