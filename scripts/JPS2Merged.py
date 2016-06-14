import os
from os.path import join

def  main():
	path = '../../Sefaria-Export/JPS'
	for root, dirs, files in os.walk(path):
		for name in files:
			print(name)
			newname = name.replace("JPS 1985 English Translation.json","merged.json")
			os.rename(join(root,name),join(root,newname))


if __name__ == '__main__':
	main()