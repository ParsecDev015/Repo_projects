"""
La idea de este proyecto es crear un programa que pruebe la conectividad de sitios web. 
Puedes usar los modulos urllib y tkinter para crear una interfaz gráfica de usuario (GUI) que 
permita a los usuarios ingresar una dirección web. Después de haber recopilado la dirección 
web del usuario, puedes pasarla a una función para devolver un código de estado HTTP para 
el sitio web actual mediante la función .getcode() del módulo urllib. 
En este ejemplo, simplemente determinamos si el código HTTP es 200. Si lo es, sabemos que 
el sitio está funcionando; de lo contrario, informamos al usuario de que no está disponible.
"""
import tkinter as tk
import urllib.request


class App(tk.Frame):
    def __init__(self, master):
        super().__init__(master, width=50)
        self.pack()
        
        # Label encima del Entry
        self.label = tk.Label(self, text="Ingrese la url que desea comprobar: ", font=("Arial", 16))
        self.label.pack(pady=10)  # Añadir padding para que no esté pegado al Entry
        
        # Entry para ingresar la URL
        self.entrythingy = tk.Entry(self, width=50, font=("TimesNewRoman", 15))
        self.entrythingy.pack(pady=10)  # Añadir padding debajo del Entry
        
        # Botón debajo del Entry
        self.button = tk.Button(self, text="Comprobar", width=10, command=self.verificar_conectividad )
        self.button.pack(pady=20)  # Añadir padding debajo del botón
        
        # Crear la variable del contenido
        self.contents = tk.StringVar()
        self.contents.set("Digite su url")
        
        # Asociar el contenido a la entrada de texto
        self.entrythingy["textvariable"] = self.contents
    
    def verificar_conectividad(self):
        url = self.contents.get()
        verificador = urllib.request.urlopen(url)
        print(f"Resultado de la verificación: {verificador.getcode()}")





root = tk.Tk()

# Establecer el tamaño de la ventana a 640x500 píxeles
root.geometry("640x500")

myapp = App(root)
myapp.mainloop()
