import requests, json, configparser, sqlite3, os, tempfile
from datetime import date, timedelta
from firebird.driver import connect, driver_config
import sys
from PyQt5 import uic, QtWidgets,QtGui,QtCore
from PyQt5.QtGui import QPixmap, QMovie
from PyQt5.QtWidgets import *
import pandas as pd

config = configparser.ConfigParser()
config.sections()
config.read('config_firebird.ini')

linha2 = config.get('local', 'host', fallback='Erro ao ler linha2')
linha3 = config.get('local', 'user', fallback='Erro ao ler linha3')
linha4 = config.get('local', 'password', fallback='Erro ao ler linha4')

linha6 = config.get('Presence', 'server', fallback='Erro ao ler linha6')
linha7 = config.get('Presence', 'database', fallback='Erro ao ler linha7')
linha8 = config.get('Presence', 'protocol', fallback='Erro ao ler linha8')
linha9 = config.get('Presence', 'charset', fallback='Erro ao ler linha9')

linha11 = config.get('Loja', 'cod_loj', fallback='Erro ao ler linha9')

# Register Firebird server
srv_cfg = "[local]" + '\n' + "host = " + linha2 + '\n' + "user = " + linha3 + '\n' + "password = "+ linha4
# print('srv_cfg = ',srv_cfg)
driver_config.register_server('local', srv_cfg)

# Register database
db_cfg = "[Presence]" + '\n' + "server = " + linha6 + '\n' + "database = " + linha7 + '\n' + "protocol = " + linha8 +  '\n' "charset = " + linha9
# print('db_cfg = ',db_cfg)
driver_config.register_database('Presence', db_cfg)
con = connect('Presence')
print("banco connectado")


#Carregando UI
app = QtWidgets.QApplication(sys.argv)
temp_dir = getattr(sys, "_MEIPASS", tempfile.gettempdir())
ui_file = os.path.join(temp_dir, "resources\_telaSeparaItens.ui")
ui_load = os.path.join(temp_dir, "resources\_loading.ui")
icon_file = os.path.join(temp_dir, "resources\icone.ico")

if os.path.exists(ui_file):
    tela = uic.loadUi(ui_file)
else:
    tela = uic.loadUi("_telaSeparaItens.ui")

if os.path.exists(ui_load):
    loadin = uic.loadUi(ui_load)
else:
    loadin = uic.loadUi("_loading.ui")

if os.path.exists(icon_file):
    tela.setWindowIcon(QtGui.QIcon(icon_file))
else:
    tela.setWindowIcon(QtGui.QIcon('icone.ico'))





print(date.today())

# Funções para filtro de menu dinamico (slotSelect / menuClose / clearFilter/ filterdata / columnfilterclicked)
def slotSelect(state):
    for checkbox in tela.checkBoxs:
        checkbox.setChecked(QtCore.Qt.Checked == state)

def menuClose():
    tela.keywords[tela.col] = []
    for element in tela.checkBoxs:
        if element.isChecked():
            tela.keywords[tela.col].append(element.text())
    filterdata()
    tela.menu.close()

def clearFilter():
    if tela.tableWidget.rowCount() > 0:
        for i in range(tela.tableWidget.rowCount()):
            tela.tableWidget.setRowHidden(i, False)

def filterdata():
    columnsShow = dict([(i, True) for i in range(tela.tableWidget.rowCount())])
    for i in range(tela.tableWidget.rowCount()):

        for j in range(tela.tableWidget.columnCount()):
            item = tela.tableWidget.item(i, j)
            if tela.keywords[j]:
                if item.text() not in tela.keywords[j]:
                    columnsShow[i] = False

    for key in columnsShow:
        tela.tableWidget.setRowHidden(key, not columnsShow[key])

def columnfilterclicked(index):
    tela.menu = QtWidgets.QMenu()
    tela.col = index
    tela.data_unique = []
    tela.checkBoxs = []
    tela.checkBox = QtWidgets.QCheckBox("Select all", tela.menu)
    checkableAction = QtWidgets.QWidgetAction(tela.menu)
    checkableAction.setDefaultWidget(tela.checkBox)
    tela.menu.addAction(checkableAction)
    tela.checkBox.setChecked(True)
    tela.checkBox.stateChanged.connect(slotSelect)

    for i in range(tela.tableWidget.rowCount()):
        if not tela.tableWidget.isRowHidden(i):
            item = tela.tableWidget.item(i, index)
            if item.text() not in tela.data_unique:
                tela.data_unique.append(item.text())
                checkBox = QtWidgets.QCheckBox(item.text(), tela.menu)
                checkBox.setChecked(True)
                checkableAction = QtWidgets.QWidgetAction(tela.menu)
                checkableAction.setDefaultWidget(checkBox)
                tela.menu.addAction(checkableAction)
                tela.checkBoxs.append(checkBox)

    btn = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel,
                                     QtCore.Qt.Horizontal, tela.menu)
    btn.accepted.connect(menuClose)
    btn.rejected.connect(tela.menu.close)
    checkableAction = QtWidgets.QWidgetAction(tela.menu)
    checkableAction.setDefaultWidget(btn)
    tela.menu.addAction(checkableAction)
    headerPos = tela.tableWidget.mapToGlobal(tela.tableWidgetHeader.pos())
    posY = headerPos.y() + tela.tableWidgetHeader.height()
    posX = headerPos.x() + tela.tableWidgetHeader.sectionPosition(index)
    tela.menu.exec_(QtCore.QPoint(posX, posY))





def bancolocal():
    try:
        con1 = sqlite3.connect('produtos.db')
        createTable =("""CREATE TABLE produtos (
                            SKU	TEXT,
                            SALDO	INTEGER,
                            PRIMARY KEY(SKU)
                        )""")
        cur1 = con1.cursor()
        cur1.execute(createTable)
        con1.commit()
        con1.close()
        print('Banco criado com tabela')
    except:
        print('deu ruim')



def separaPresence():
    tela.setEnabled(False)
    tela.status.setText('Processando...')
    QApplication.processEvents()

    con2 = sqlite3.connect('produtos.db')
    cur2 = con2.cursor()
    data_max = ("SELECT max(DATA) AS data FROM data_log")
    cur2.execute(data_max)
    for ultima_data in cur2.fetchall():
        data_check = ultima_data[0]

    d1 = str(date.today())
    print(data_check)
    print(type(data_check))

    print(d1)
    print(type(d1))

    if data_check == d1:
        query = ('SELECT * FROM log')
        df_separa = pd.read_sql(query, con2, index_col=None)
        df_separa = df_separa.drop(columns=['index'])

        print(df_separa)

        # Obtém uma referência ao objeto TableWidget
        tabela = tela.findChild(QtWidgets.QTableWidget, "tableWidget")

        # Define as colunas da tabela
        tabela.setColumnCount(len(df_separa.columns))
        tabela.setHorizontalHeaderLabels(df_separa.columns)

        # Define o número de linhas da tabela
        tabela.setRowCount(len(df_separa))
        totalrow = len(df_separa)
        print(totalrow)
        basecalculo = totalrow / 100
        countrow = 0
        # Preenche a tabela com os dados do DataFrame
        loadin.show()
        QApplication.processEvents()
        for row in range(len(df_separa)):
            countrow = countrow +1
            percent = round(countrow / basecalculo, 0)
            loadin.progressBar.setValue(int(percent))
            QApplication.processEvents()
            print(countrow)
            for col in range(len(df_separa.columns)):
                item = QtWidgets.QTableWidgetItem(str(df_separa.iloc[row, col]))
                tabela.setItem(row, col, item)

        # Adequa as funções de Filtro dinamico
        tela.tableWidgetHeader = tela.tableWidget.horizontalHeader()
        tela.tableWidgetHeader.sectionClicked.connect(columnfilterclicked)
        tela.keywords = dict([(i, []) for i in range(tela.tableWidget.columnCount())])
        tela.checkBoxs = []
        tela.col = None
        loadin.close()

    else:

        print('oi')
        con = connect('Presence')
        loadin.show()
        QApplication.processEvents()

        query =(" SELECT  "+
                " LE.cod_loj, "+
                " LE.loja, "+
                " LE.controlada, "+
                " LE.estado, "+
                " ( "+
                " case "+
                " WHEN LE.estado in ('AM','RR','AP','PA','TO','RO','AC')  then 'NORTE' "+
                " WHEN LE.estado in ('MA','PI','CE','RN','PE','PB','SE','AL','BA')  then 'NORDESTE' "+
                " WHEN LE.estado in ('MT','MS','GO','DF')  then 'CENTRO-OESTE' "+
                " WHEN LE.estado in ('SP','RJ','ES','MG')  then 'SULDESTE' "+
                " WHEN LE.estado in ('PR','RS','SC')  then 'SUL' "+
                " ELSE 'ESTADO INCORRETO' "+
                " END "+
                " ) AS REGIAO, "+
                " I.NUMERO, "+
                " P.situacao, "+
                " ( "+
                " case "+
                " when LE.estado in ('AM','RR','AP','PA','TO','RO','AC') then  1 "+
                " WHEN LE.estado in ('MA','PI','CE','RN','PE','PB','SE','AL','BA')  then 2 "+
                " when LE.cod_rede  = '04'    then 3 "+
                " when LE.numero_serie ='1696'    then 4 "+
                " WHEN LE.estado in ('MT','MS','GO','DF')  then 5 "+
                " WHEN LE.estado in ('PR','RS','SC')  then 6 "+
                " WHEN LE.estado in ('SP','RJ','ES','MG')  then 7 "+
                " else 8 end "+
                "   ) as prioridade, "+
                " ( "+
                " case "+
                " WHEN P.situacao = '1' then 'EM EDIÇÃO' "+
                " WHEN P.situacao = '2' then 'REGISTRADO' "+
                " WHEN P.situacao = '3' then 'APROVADO' "+
                " WHEN P.situacao = '4' then 'BLOQUEADO' "+
                " WHEN P.situacao = '5' then 'COM FATURAMENTO' "+
                " WHEN P.situacao = '6' then 'CONCLUIDO' "+
                " WHEN P.situacao = '7' then 'CANCELADO' "+
                " ELSE 'SITUAÇÃO NÃO PROGRAMADA' "+
                " END "+
                " ) AS SIT, "+
                " LE.cod_rede, "+
                " RD.nome AS REDE, "+
                " P.DT_CADASTRO,  "+
                " I.CONTROLE_ITEM,  "+
                " I.CODIGO,  "+
                " trim(I.CODIGO)||I.TAMANHO||I.COR as sku, "+
                " ax.ean13 as sku_totvs, "+
                " VP.DESCRICAO,  "+
                " I.TAMANHO,  "+
                " VP.TAMANHO TAM_DESC,  "+
                " I.COR,  "+
                " VP.COR COR_DESC,  "+
                " I.QT as QT_ORIGINAL, "+
                " I.SALDO as qt, "+
                " I.QUANTIDADE_SEPARADA,  "+
                " P.ESTADO_SEPARACAO, "+
                " P.COD_SEPARADOR, S.NOME AS SEPARADOR,  "+
                " P.COD_PEDIDO_EXTERNO CODIGO_EXTERNO "+
                " FROM PED_HEADER P  "+
                " JOIN PED_ITENS I ON I.SERIE_PD = P.SERIE_PD AND I.NUMERO = P.NUMERO  "+
                " JOIN LOJB010 LE ON LE.COD_LOJ       = P.cli_loja "+
                " LEFT JOIN V_PRODUTO_GRADE VP ON VP.CODIGO = I.CODIGO AND VP.TAMPOS = I.TAMANHO AND VP.CORPOS = I.COR  "+
                " LEFT JOIN LOJB011 S ON S.CONTROLE = P.COD_SEPARADOR "+
                " INNER JOIN LOJB006 PRD ON PRD.CODIGO = I.CODIGO "+
                " LEFT JOIN LOJB032  RD ON LE.cod_rede = RD.cod_rede "+
                " left join lojb024 ax on ax.codigo = i.codigo and ax.tamanho = i.tamanho and ax.posicao =i.cor and ax.principal='S' "+
                " WHERE ((P.EXCLUIDO = 'N') OR (P.EXCLUIDO IS NOT NULL) ) "+
                " AND   P.PRODUTO_SERVICO   = 'P'  "+
                " AND   P.SITUACAO IN (3,5)  "+
                " AND    P.ESTADO_SEPARACAO in ( '1','2','3','4' ) "+
                " AND  (P.DT_CADASTRO  BETWEEN '01.01.2023 00:00:00' AND '31.12.2099 23:59:59') "+
                " AND  (P.DT_APROVACAO BETWEEN '11.11.1911 00:00:00' AND '31.12.2099 23:59:59') "+
                " AND   LE.CONTROLADA <> 'D'  "+
                " ORDER BY  PRIORIDADE, I.SERIE_PD, I.NUMERO, I.CONTROLE_ITEM "
                )

        df_separa = pd.read_sql(query, con, index_col=None)
        df_separa.loc[:, 'SALDO'] = ''
        df_separa.loc[:, 'DATA_PROCESSAMENTO'] = ''

        con3 = sqlite3.connect('produtos.db')
        cur3 = con3.cursor()
        zera = ("DROP TABLE log")
        print(zera)
        try:
            cur3.execute(zera)
            con3.commit()
            con3.close()
        except:
            pass
        basecalculo = len(df_separa.index) / 80

        for ind in df_separa.index:

            percent = round(ind / basecalculo, 0)
            print(percent)
            loadin.progressBar.setValue(int(percent))
            print(ind)
            QApplication.processEvents()
            ST = (df_separa['SKU_TOTVS'][ind])
            QT = int((df_separa['QT'][ind]))
            con3 = sqlite3.connect('produtos.db')
            cur3 = con3.cursor()

            SELECT = ("select SALDO from produtos WHERE SKU ='"+str(ST)+"'")
            print(SELECT)
            cur3.execute(SELECT)
            for st_saldo in cur3.fetchall():
                s = st_saldo[0]
                df_separa.loc[ind, 'SALDO'] = s
                df_separa.loc[ind, 'DATA_PROCESSAMENTO'] = date.today()
                novosaldo = str(int(s) - QT)
                cur4 = con3.cursor()
                updatesaldo = "UPDATE produtos SET saldo ='"+novosaldo+"' WHERE sku ='"+ST+"'"
                cur4.execute(updatesaldo)
                con3.commit()
                print('saldo atualizado')



            con3.commit()
            con3.close()

        print(df_separa)

        # Obtém uma referência ao objeto TableWidget
        tabela = tela.findChild(QtWidgets.QTableWidget, "tableWidget")

        # Define as colunas da tabela
        tabela.setColumnCount(len(df_separa.columns))
        tabela.setHorizontalHeaderLabels(df_separa.columns)

        # Define o número de linhas da tabela
        tabela.setRowCount(len(df_separa))

        # Preenche a tabela com os dados do DataFrame
        for row in range(len(df_separa)):
            for col in range(len(df_separa.columns)):
                item = QtWidgets.QTableWidgetItem(str(df_separa.iloc[row, col]))
                tabela.setItem(row, col, item)
        loadin.progressBar.setValue(90)
        QApplication.processEvents()

        # Adequa as funções de Filtro dinamico
        tela.tableWidgetHeader = tela.tableWidget.horizontalHeader()
        tela.tableWidgetHeader.sectionClicked.connect(columnfilterclicked)
        tela.keywords = dict([(i, []) for i in range(tela.tableWidget.columnCount())])
        tela.checkBoxs = []
        tela.col = None


        con3 = sqlite3.connect('produtos.db')
        df_separa.to_sql(name='log', con=con3)
        insertdata =("INSERT INTO Data_log (DATA) VALUES ('"+str(date.today())+"')")
        cur3 = con3.cursor()
        loadin.progressBar.setValue(100)
        QApplication.processEvents()
        cur3.execute(insertdata)
        con3.commit()
        con3.close()
        loadin.close()
    tela.setEnabled(True)

    separaPresence.df = df_separa



separaPresence.df = pd.DataFrame()

def saldo():
    tela.setEnabled(False)
    count = 1
    tela.status.setText('Processando...')
    QApplication.processEvents()

    #Variavel de conexão global
    con = ''
    con = connect('Presence')
    cur = con.cursor()
    con3 = sqlite3.connect('produtos.db')
    cur3 = con3.cursor()
    zera = ("DELETE FROM produtos")
    print(zera)
    cur3.execute(zera)
    con3.commit()
    con3.close()

    loadin.show()
    loadin.progressBar.setValue(1)
    QApplication.processEvents()


    queryproduto = (" SELECT  "+
                    " trim(I.CODIGO)||I.TAMANHO||I.COR as sku, "+
                    " AX.ean13 AS SKU_TOTVS "+
                    " FROM PED_HEADER P  "+
                    " JOIN PED_ITENS I ON I.SERIE_PD = P.SERIE_PD AND I.NUMERO = P.NUMERO  "+
                    " JOIN LOJB010 L ON L.COD_LOJ         = P.LOJA_EMITENTE  "+
                    " JOIN LOJB010 LE ON LE.COD_LOJ       = P.cli_loja "+
                    " LEFT JOIN V_PRODUTO_GRADE VP ON VP.CODIGO = I.CODIGO AND VP.TAMPOS = I.TAMANHO AND VP.CORPOS = I.COR  "+
                    " LEFT JOIN LOJB017 E ON E.COD_LOJ = P.LOJA AND E.CODIGO = I.CODIGO AND E.TAMANHO = I.TAMANHO AND E.COR = I.COR  "+
                    " LEFT JOIN LOJB011 S ON S.CONTROLE = P.COD_SEPARADOR "+
                    " INNER JOIN LOJB006 PRD ON PRD.CODIGO = I.CODIGO "+
                    " INNER JOIN LOJB024 AX on AX.codigo = I.CODIGO AND AX.tamanho = I.tamanho AND AX.posicao = I.cor AND AX.principal ='S' "+
                    " WHERE ((P.EXCLUIDO = 'N') OR (P.EXCLUIDO IS NOT NULL) ) "+
                    " AND   P.PRODUTO_SERVICO   = 'P'  "+
                    " AND   P.SITUACAO IN (3,5)  "+
                    " AND    P.ESTADO_SEPARACAO in ( '1','2','3','4' ) "+
                    " AND  (P.DT_CADASTRO  BETWEEN '01.01.2023 00:00:00' AND '31.12.2099 23:59:59') "+
                    " AND  (P.DT_APROVACAO BETWEEN '11.11.1911 00:00:00' AND '31.12.2099 23:59:59') "+
                    " AND   L.CONTROLADA <> 'D'  "+
                    " AND   LE.CONTROLADA <> 'D'  "+
                    " group by sku, SKU_TOTVS "+
                    " ORDER BY sku ")

    ft = open("horario.txt", "w", newline="")
    ft.close()
    urlToken = 'https://www30.bhan.com.br:9443/api/totvsmoda/authorization/v2/token'
    urlSaldo = 'https://www30.bhan.com.br:9443/api/totvsmoda/product/v2/balances/search'

    keys = {'grant_type': 'password',
            'client_id': 'piticasapiv2',
            'client_secret': '7977975229',
            'username': 'INT',
            'password': '21499100',
            'branch': '1'}

    TokenP1 = requests.post(urlToken, data=keys)
    TokenP2 = json.loads(TokenP1.text)
    Token = "Bearer " + TokenP2["access_token"]
    print(Token)

    headersaldo = {"Authorization": Token, "Content-Type": "application/json"}

    header = ['SKU', 'SALDO']
    df_1 = pd.DataFrame(columns=header)

    cur.execute(queryproduto)
    loadin.progressBar.setValue(5)

    cur.fetchall()
    totalrecord = cur.rowcount
    print(totalrecord)
    basecalculo = totalrecord / 100
    QApplication.processEvents()
    contrecord =0
    cur.execute(queryproduto)
    for listSKU in cur.fetchall():
        contrecord = contrecord +1
        percent = round(contrecord / basecalculo, 0)
        percent = percent +5
        print(percent)
        loadin.progressBar.setValue(int(percent))
        QApplication.processEvents()
        SKU_PRES = listSKU[0]
        SKU_TOTVS = listSKU[1]
        print(SKU_TOTVS)

        json_data = {
            'filter': {
                'productCodeList': [
                    int(SKU_TOTVS),
                ],
            },
            'option': {
                'balances': [
                    {
                        'branchCode': 3,
                        'stockCodeList': [
                            1,
                        ],
                        'isSalesOrder': True,
                        'isTransaction': True,
                        'isProductionPlanning': False,
                        'isPurchaseOrder': False,
                        'isProductionOrder': False,
                    },
                ],
            },
            'page': 1,
            'pageSize': 20,
            'order--': 'string',
        }
        saldoresponse = requests.post(url=urlSaldo, json=json_data, headers=headersaldo)

        jsonsaldo = json.loads(saldoresponse.text)
        jsonitens = jsonsaldo["items"]
        print(jsonitens)

        for itens in jsonitens:

            if ("00" + str(count))[-2:] == '00':
                print("estamos no item numero: " + str(count))
            else:
                pass
            #print(itens['balances'])
            balance = itens['balances']
            # print(itens['referenceCode'] + " - "+ itens['productName'])
            produto = itens['productName']
            skuTotvs = itens['referenceCode']
            # print(skuTotvs)

            for subitens in balance:
                # print("branchCode: " + str(subitens['branchCode']))
                # print("stockCode: " + str(subitens['stockCode']))
                # print("stock: " + str(subitens['stock']))
                # print("productionOrderProgress: " + str(subitens['productionOrderProgress']))
                # print("salesOrder: " +str( subitens['salesOrder']))
                branchCode = subitens['branchCode']
                stockCode = subitens['stockCode']
                stock = subitens['stock']
                stock = str(stock).replace('None','0')
                stock = int(stock)
                transaction = subitens['outputTransaction']
                transaction = str(transaction).replace('None','0')
                transaction = int(transaction)
                productionOrderProgress = subitens['productionOrderProgress']
                productionOrderProgress = str(productionOrderProgress).replace('None','0')
                productionOrderProgress = int(productionOrderProgress)
                salesOrder = subitens['salesOrder']

                saldo_totvs = (stock - salesOrder - transaction)

                try:
                    con3 = sqlite3.connect('produtos.db')
                    cur3 = con3.cursor()
                    insert = ("INSERT INTO produtos  (SKU, SALDO) VALUES('" + SKU_TOTVS + "','" + str(saldo_totvs)+"') ")
                    print(insert)
                    cur3.execute(insert)
                    con3.commit()
                    con3.close()
                except:
                    print('vish')


    print('deu bom')
    loadin.close()
    loadin.progressBar.setValue(0)
    con.close()
    tela.setEnabled(True)
    tela.status.setText('Processo concluido com sucesso')


def exporta():
    tela.status.setText('CARREGANDO')
    tela.setEnabled(False)
    QApplication.processEvents()
    #pd.set_option('display.float_format', lambda x: '{:.2f}'.format(x).replace('.', ',') % x)

    if separaPresence.df.empty == True:
        #error.show()
        #error.msg.setText('APERTA O SHAZAM')

        tela.status.setText('POR FAVOR CARREGUE OS DADOS')
    else:
        #pd.options.display.float_format = '${:, .2f}'.format
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        fileName, _ = QFileDialog.getSaveFileName(tela, "Salvar como", "",
                                                  "All Files (*);;Pasta de Trabalho do Excel(*.xlsx)", options=options)
        if fileName:
            print(fileName)
        else:
            print('deuruim')
        try:
            writer = pd.ExcelWriter(fileName + '.xlsx', engine='xlsxwriter')
            separaPresence.df.to_excel(writer, sheet_name='Separa', index=False)
            writer.s
            writer.save()
            writer.close()
            tela.status.setText('EXEL GERADO COM SUCESSO')
        except:
            tela.status.setText('FALHA AO GERAR O EXCEL')

    tela.setEnabled(True)

def testebotao():
    print('djsfhldsjfhçjlfçsnfjlsf')


tela.excelButton.clicked.connect(exporta)
tela.pushButton.clicked.connect(bancolocal)
tela.pushButton_2.clicked.connect(saldo)
tela.pushButton_3.clicked.connect(separaPresence)



tela.show()
app.exec()