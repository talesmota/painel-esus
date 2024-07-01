import customtkinter as ctk
from customtkinter import CTkProgressBar, IntVar
import time
import os.path
from PIL import Image
from src.infra.db.settings.connection import DBConnectionHandler
import pandas as pd
import logging
logging.basicConfig(level=logging.DEBUG)

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("dark-blue")

def CenterWindowToDisplay(Screen: ctk, width: int, height: int, scale_factor: float = 1.0):
    """Centers the window to the main display/monitor"""
    screen_width = Screen.winfo_screenwidth()
    screen_height = Screen.winfo_screenheight()
    x = int(((screen_width/2) - (width/2)) * scale_factor)
    y = int(((screen_height/2) - (height/1.5)) * scale_factor)
    return f"{width}x{height}+{x}+{y}"

root = ctk.CTk()
root.iconbitmap('icon/Icon_Painel_Purple_ICO.ico')
root.title('Configurar')
root.geometry(CenterWindowToDisplay(root, 800, 600, root._get_window_scaling()))


def startProgressBar(window):
    myValC = IntVar()
    progress_bar = CTkProgressBar(window, width=1000, variable=myValC)

    progress_bar.grid(column=0, row=1, padx=100, pady=100)

    steps = 25
    progress_bar.set(0)
    progress_bar_val = 1/steps
    step_val = 0
    for i in range(steps):
        time.sleep(0.25)
        step_val += progress_bar_val
        progress_bar.set(step_val)
        progress_bar.update_idletasks()
    progress_bar.destroy()


def connectDB(new_window):
        with DBConnectionHandler() as db_con:
                engine = db_con.get_engine()
                res = pd.read_sql_query("select * from information_schema.tables", con=engine)

                image_path = os.getcwd()
                image_path = os.path.join(image_path, 'icon/success.png')
                
                my_image = ctk.CTkImage(light_image=Image.open(image_path), dark_image=Image.open(image_path),
                                        size=(30, 30))
                image_label = ctk.CTkLabel(master=new_window, text="", font=("arial bold", 20), image=my_image)
                image_label.pack(pady=10)

                label = ctk.CTkLabel(master=new_window, text="Conexão realizada com sucesso!", font=("arial bold", 20))
                label.pack(pady=12, padx=10)

def connectDBWithParams(new_window, dbUser, dbPassword, dbHost, dbPort, dbDatabase):
        with DBConnectionHandler(dbUser, dbPassword, dbHost, dbPort, dbDatabase) as db_con:
                engine = db_con.get_engine()
                res = pd.read_sql_query("select * from information_schema.tables", con=engine)
                print("CONECTOU", res.shape)

                image_path = os.getcwd()
                image_path = os.path.join(image_path, 'icon/success.png')
                
                my_image = ctk.CTkImage(light_image=Image.open(image_path), dark_image=Image.open(image_path),
                                        size=(30, 30))
                image_label = ctk.CTkLabel(master=new_window, text="", font=("arial bold", 20), image=my_image)
                image_label.pack(pady=10)

                label = ctk.CTkLabel(master=new_window, text="Conexão realizada com sucesso!", font=("arial bold", 20))
                label.pack(pady=12, padx=10)

def connectionErrorDB(new_window):
        print("ERRO DE CONEXÃO")
        image_path = os.getcwd()
        image_path = os.path.join(image_path, 'icon/error.png')
        
        my_image = ctk.CTkImage(light_image=Image.open(image_path), dark_image=Image.open(image_path),
                                size=(30, 30))
        image_label = ctk.CTkLabel(master=new_window, text="", font=("arial bold", 20), image=my_image)
        image_label.pack(pady=10)

        error_label = ctk.CTkLabel(master=new_window, text="Erro ao conectar ao banco de dados. Por favor, realize o procedimento de configuração novamente ou entre em contato com o suporte!", 
                        font=("arial bold", 15),
                        width=350,
                        height=25,
                        corner_radius=5,
                        anchor="center",
                        wraplength=350)
        error_label.pack(pady=10, padx=10)      

def topLevelViewConeectionFunction(frame, dbUser, dbPassword, dbHost, dbPort, dbDatabase):
        new_window = ctk.CTkToplevel(frame)
        new_window.title("Hello There!")
        new_window.geometry(CenterWindowToDisplay(new_window, 400, 200, new_window._get_window_scaling()))
        # new_window.geometry("400x200")
        new_window.grab_set()

        def close():
                new_window.destroy()
                new_window.update()

        try:
                if(dbUser and dbPassword  and dbHost  and dbPort and dbDatabase ):
                        print(dbUser, ' ', dbPassword , ' ', dbHost , ' ', dbPort, ' ', dbDatabase )
                        new_window.after(1, connectDBWithParams(new_window, dbUser, dbPassword, dbHost, dbPort, dbDatabase))
                else:
                        new_window.after(1, connectDB(new_window))
        except Exception as error:
                logging.exception(error)
                new_window.after(1, connectionErrorDB(new_window))

        closeButton = ctk.CTkButton(master=new_window, text="Fechar", command=close)
        closeButton.pack(pady=12, padx=10)

def startTopLevelViewConnection(frame, dbUser, dbPassword, dbHost, dbPort, dbDatabase): 
        frame.after(1000, topLevelViewConeectionFunction(frame, dbUser, dbPassword, dbHost, dbPort, dbDatabase))


def existsEnv(root):
        frame = ctk.CTkFrame(master=root,  height=800, width=600)
        frame.pack(fill="both", expand=True)

        label = ctk.CTkLabel(master=frame, text="Painel Esus", font=("arial bold", 50))
        label.pack(pady=12, padx=10)
        
        image_path = os.getcwd()
        image_path = os.path.join(image_path, 'icon/env.png')
        
        my_image = ctk.CTkImage(light_image=Image.open(image_path), dark_image=Image.open(image_path),
                                size=(100, 100))
        image_label = ctk.CTkLabel(master=frame, text="", font=("arial bold", 20), image=my_image)
        image_label.pack(pady=10)

        exist_file_label = ctk.CTkLabel(master=frame, text="Arquivo de configuração já existente!",
                                font=("arial bold", 15), 
                                width=600,
                                height=25,
                                corner_radius=5,
                                anchor="center",
                                wraplength=600)
        exist_file_label.pack(pady=10, padx=10)

        choose_option = ctk.CTkLabel(master=frame, text="Você pode testar novamente a conexão com o banco de dados ou criar uma nova configuração do Painel-Esus. Escolha a opção abaixo:", 
                                font=("arial bold", 15),
                                width=600,
                                height=25,
                                corner_radius=5,
                                anchor="center",
                                wraplength=600)
        choose_option.pack(pady=10, padx=10)

        test_connection_button = ctk.CTkButton(master=frame, text="Testar conexão", command=lambda: startTopLevelViewConnection(frame, None, None, None, None, None))
        test_connection_button.pack(pady=12, padx=10)

        def close():
                frame.destroy()
                frame.update()
                tabs()

        create_new_env_button = ctk.CTkButton(master=frame, text="Configurar novamente", command=close)
        create_new_env_button.pack(pady=12, padx=10)


def buildEnvStr(env):
        if(env != None):
                strValue = str(env)
                new_str = strValue.replace('\n', '')
                # new_str = strValue.replace("'","")
                return new_str
        return env

def createEnv(input_host, input_database, input_user, input_password, input_port, input_cidade, input_estado, 
              input_user_admin, input_password_admin, input_populacao, input_bridge_login_url):
        with open(".env", "w",encoding='utf-8') as f:
                lines = [
                                "DB_HOST='"+buildEnvStr(input_host.get())+"'\n",
                                "DB_DATABASE='"+buildEnvStr(input_database.get())+"'\n",
                                "DB_USER='"+buildEnvStr(input_user.get())+"'\n",
                                "DB_PASSWORD='"+buildEnvStr(input_password.get())+"'\n",
                                "DB_PORT='"+buildEnvStr(input_port.get())+"'\n",
                                "CIDADE='"+buildEnvStr(input_cidade.get())+"'\n",
                                "ESTADO='"+buildEnvStr(input_estado.get())+"'\n",
                                "ADMIN_USERNAME='"+buildEnvStr(input_user_admin.get())+"'\n",
                                "ADMIN_PASSWORD='"+buildEnvStr(input_password_admin.get())+"'\n",
                                "POPULATION="+input_populacao.get()+"\n",
                                "PASSWORD_SALT='"+'painel'+"'\n",
                                "BRIDGE_LOGIN_URL='"+buildEnvStr(input_bridge_login_url.get())+"'\n",
                                "RELOAD_BASE_SCHELDULE='4:00'"+"\n",
                                "ARTEFACT="+'web'+"\n",
                                "ENV="+'instalador'+"\n",
                                "SECRET_TOKEN="+'111111111111111111111'+"\n",
                                
                        ]
                f.writelines(lines)
                f.close()

class Field():
   def __init__(self, field):
     self.field = field
     self.index = 0
     self.value = ''
   
   def getFieldIndex(self): 
     choices = {'DB_HOST': 0, 'DB_DATABASE': 1, 'DB_USER': 2, 'DB_PASSWORD': 3,
                'DB_PORT': 4, 'CIDADE': 5, 'ESTADO': 6, 'ADMIN_USERNAME': 7, 
                'ADMIN_PASSWORD': 8, 'POPULATION': 9 
               }
     split = self.field.split("=")
     if(len(split) > 1):
        self.index = choices.get(split[0])
        self.value =split[1]
     else:
        self.index = None
        self.value = None

def fillInputFields(inputFieldsArray: [any]): 
        if os.path.exists('.env'):
                fileContent = any
                with open(".env", "r") as file:
                        fileContent = file.readlines()
                for input in fileContent:
                        field = Field(input)
                        field.getFieldIndex()
                        if(field.index != None):
                                envField = field.value.replace("'","").strip()
                                if len(envField) > 0:
                                        inputFieldsArray[field.index].insert(0, field.value.replace("'","").strip())

                file.close()
        else:
                return

def successFrame():
        frame = ctk.CTkFrame(master=root,  height=300, width=600)
        frame.pack(fill="both", expand=True)

        blank_label = ctk.CTkLabel(master=frame,
                                text="",
                        width=600,
                        height=25,
                        corner_radius=5,
                        anchor="center",
                        wraplength=600)
        blank_label.pack(pady=10, padx=10)

        label = ctk.CTkLabel(master=frame, text="Configuração realizada com sucesso!", font=("arial bold", 25))
        label.pack(pady=12, padx=10)
        
        image_path = os.getcwd()
        image_path = os.path.join(image_path, 'icon/success.png')
        
        my_image = ctk.CTkImage(light_image=Image.open(image_path), dark_image=Image.open(image_path),
                                size=(100, 100))
        image_label = ctk.CTkLabel(master=frame, text="", font=("arial bold", 20), image=my_image)
        image_label.pack(pady=10)

        blank_label = ctk.CTkLabel(master=frame,
                                   text="",
                                width=600,
                                height=25,
                                corner_radius=5,
                                anchor="center",
                                wraplength=600)
        blank_label.pack(pady=10, padx=10)

        exist_file_label = ctk.CTkLabel(master=frame, text="A configuração do Painel Esus foi realizada com sucesso. \n Para iniciar o sistema e começar a usufruir dos paineis disponibilizados, basta fechar essa janela de configuração e iniciar o executável do Painel Esus presente na mesma pasta.",
                                font=("arial bold", 15), 
                                width=600,
                                height=25,
                                corner_radius=5,
                                anchor="center",
                                wraplength=600)
        exist_file_label.pack(pady=10, padx=10)

        choose_option = ctk.CTkLabel(master=frame, text="Caso aconteça algum problema na hora da execução do Painel Esus, reinicie o processo de configuração validando os dados de aceso à base de dados.", 
                                font=("arial bold", 15),
                                width=600,
                                height=25,
                                corner_radius=5,
                                anchor="center",
                                wraplength=600)
        choose_option.pack(pady=10, padx=10)


        create_new_env_button = ctk.CTkButton(master=frame, text="Fechar", command=root.destroy)
        create_new_env_button.pack(pady=12, padx=10)


def tabs():
        tabview = ctk.CTkTabview(root, width=780, height=580)
        tabview.pack()
        tabview.add("Banco de dados")
        tabview.add("Painel E-sus")
        tabview.tab("Banco de dados").grid_columnconfigure(0, weight=1)
        tabview.tab("Painel E-sus").grid_columnconfigure(0, weight=1)


        # --------------------------------CONFIGURAÇÃO BANCO DE DADOS--------------------------------
        frame = ctk.CTkFrame(tabview.tab("Banco de dados"), height=780, width=580)
        frame.pack(fill="both", expand=True)
        
        label = ctk.CTkLabel(master=frame, text="Configuração do Banco de dados", font=("arial bold", 25))
        label.pack(pady=12, padx=10)

        label_info = ctk.CTkLabel(master=frame, text="Por favor, siga todos os passos das abas apresentadas para configurar o Painel esus. \n Preencha todos os campos abaixo solicitados para a configuração da base de dados do município.", font=("arial bold", 15))
        label_info.pack(pady=12, padx=10)

        image_path = os.getcwd()
        image_path = os.path.join(image_path, 'icon/database.png')
        my_image = ctk.CTkImage(light_image=Image.open(image_path), dark_image=Image.open(image_path),
                                size=(100, 100))
        image_label = ctk.CTkLabel(master=frame, text="", font=("arial bold", 20), image=my_image)
        image_label.pack(pady=10)

        input_host = ctk.CTkEntry(master=frame, placeholder_text="Host:", width=600,
                                height=25,
                                corner_radius=10)
        input_host.pack(pady=10, padx=10)

        input_database = ctk.CTkEntry(master=frame, placeholder_text="Base de dados:", width=600,
                                height=25,
                                corner_radius=10)
        input_database.pack(pady=10, padx=10)

        input_user = ctk.CTkEntry(master=frame, placeholder_text="Usuário do banco de dados:", width=600,
                                height=25,
                                corner_radius=10)
        input_user.pack(pady=10, padx=10)

        input_password = ctk.CTkEntry(master=frame, 
                                     placeholder_text="Senha do banco de dados:",
                                     show="*",
                                     width=600,
                                     height=25,
                                     corner_radius=10)
        input_password.pack(pady=10, padx=10)

        input_port = ctk.CTkEntry(master=frame, placeholder_text="Porta do banco de dados:", width=600,
                                height=25,
                                corner_radius=10)
        input_port.pack(pady=10, padx=10)
        
        test_connection_button = ctk.CTkButton(master=frame, text="Testar conexão", command=lambda: startTopLevelViewConnection(frame, input_user.get().strip(), input_password.get().strip(),input_host.get().strip(), input_port.get().strip(), input_database.get().strip()))
        test_connection_button.pack(pady=12, padx=10)

        # --------------------------------CONFIGURAÇÃO PAINEL--------------------------------
        frame_painel = ctk.CTkFrame(tabview.tab("Painel E-sus"), height=780, width=580)
        frame_painel.pack(fill="both", expand=True)

        image_path_painel = os.getcwd()
        image_path_painel = os.path.join(image_path_painel, 'icon/painel.png')
        painel_image = ctk.CTkImage(light_image=Image.open(image_path_painel), dark_image=Image.open(image_path_painel),
                                size=(130, 100))

        label_config_painel = ctk.CTkLabel(master=frame_painel, text="Configuração do painel:", font=("arial bold", 25))
        label_config_painel.pack(pady=12, padx=10)

        label_info_painel = ctk.CTkLabel(master=frame_painel, text="Preencha todos os campos abaixo solicitados para a configuração dos dados de acesso ao painel-esus. \n Os campos 'Usuário' e 'Senha' aqui apresentados serão utilizados para fazer login na plataforma.", font=("arial bold", 15))
        label_info_painel.pack(pady=12, padx=10)

        image_label_painel = ctk.CTkLabel(master=frame_painel, text="", font=("arial bold", 20), image=painel_image)
        image_label_painel.pack(pady=10)

        input_cidade = ctk.CTkEntry(master=frame_painel, placeholder_text="Cidade:", width=600,
                                height=25,
                                corner_radius=10)
        input_cidade.pack(pady=10, padx=10)

        input_estado = ctk.CTkEntry(master=frame_painel, placeholder_text="Estado:", width=600,
                                height=25,
                                corner_radius=10)
        input_estado.pack(pady=10, padx=10)

        input_user_admin = ctk.CTkEntry(master=frame_painel, placeholder_text="Usuário de acesso ao painel-esus:", width=600,
                                height=25,
                                corner_radius=10)
        input_user_admin.pack(pady=10, padx=10)

        input_password_admin = ctk.CTkEntry(master=frame_painel, 
                                        placeholder_text="Senha de acesso ao painel-esus:", 
                                        show="*",
                                        width=600,
                                height=25,
                                corner_radius=10)
        input_password_admin.pack(pady=10, padx=10)

        input_populacao = ctk.CTkEntry(master=frame_painel, 
                                      placeholder_text="Tamanho da população (Ex: 20000):", 
                                      width=600,
                                      height=25,
                                      corner_radius=10
                                      )
        input_populacao.pack(pady=10, padx=10)

        input_bridge_login_url = ctk.CTkEntry(master=frame_painel, 
                                      placeholder_text="Url de login:", 
                                      width=600,
                                      height=25,
                                      corner_radius=10
                                      )
        input_bridge_login_url.pack(pady=10, padx=10)

        fillInputFields([input_host, input_database, input_user,input_password, input_port, input_cidade, input_estado, input_user_admin, input_password_admin, input_populacao, input_bridge_login_url])
        def close():
                createEnv(input_host, input_database, input_user,input_password, input_port, input_cidade, input_estado, input_user_admin, input_password_admin, input_populacao, input_bridge_login_url)
                tabview.destroy()
                tabview.update()
                frame_painel.destroy()
                frame_painel.update()
                successFrame()


        create_new_env_button = ctk.CTkButton(master=frame_painel, text="Finalizar configuração", command=close)
        create_new_env_button.pack(pady=12, padx=10)


if os.path.exists('.env'):
        existsEnv(root)
else:
        tabs()

root.mainloop()