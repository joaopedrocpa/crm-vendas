import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import random
import string
import re
import time

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="CRM Master 21.0", layout="wide")
URL_LOGO = "https://cdn-icons-png.flaticon.com/512/9187/9187604.png"

# --- CSS (VISUAL DARK PREMIUM) ---
st.markdown("""
<style>
    [data-testid="stSidebar"] {min-width: 300px;}
    div[data-testid="stMetric"] {
        background-color: #262730;
        border: 1px solid #464b5c;
        padding: 15px;
        border-radius: 8px;
    }
    div[data-testid="stMetricLabel"] {color: #b0b3b8 !important; font-weight: bold;}
    div[data-testid="stMetricValue"] {color: #ffffff !important;}
    .stButton button {width: 100%; font-weight: bold;}
    .stProgress > div > div > div > div {background-color: #00ff00;}
</style>
""", unsafe_allow_html=True)

# --- FUNÇÕES NÚCLEO ---

def gerar_id_proposta():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))

def extrair_id(texto):
    if pd.isna(texto): return None
    match = re.search(r'(#[A-Z0-9]{4})', str(texto))
    return match.group(1) if match else None

def extrair_pedido_protheus(texto):
    if pd.isna(texto): return None
    match = re.search(r'\[PROTHEUS\] Pedido: (\w+)', str(texto))
    return match.group(1) if match else None

def limpar_valor_inteiro(valor):
    if pd.isna(valor) or str(valor).strip() == '': return 0
    if isinstance(valor, (int, float)): return int(valor)
    s = str(valor).upper().replace('R$', '').strip()
    if ',' in s: s = s.split(',')[0]
    s = s.replace('.', '')
    s = re.sub(r'[^\d]', '', s)
    if not s: return 0
    return int(s)

def formatar_moeda_visual(valor):
    if pd.isna(valor): return "R$ 0"
    try:
        val = int(valor)
        return f"R$ {val:,.0f}".replace(',', '.')
    except: return "R$ 0"

def formatar_documento(valor):
    if pd.isna(valor) or str(valor).strip() == '': return "-"
    doc = ''.join(filter(str.isdigit, str(valor)))
    if not doc: return "-"
    if len(doc) > 11:
        doc = doc.zfill(14)
        return f"{doc[:2]}.{doc[2:5]}.{doc[5:8]}/{doc[8:12]}-{doc[12:]}"
    else:
        doc = doc.zfill(11)
        return f"{doc[:3]}.{doc[3:6]}.{doc[6:9]}-{doc[9:]}"

def formatar_data_br(data):
    if pd.isna(data) or str(data).strip() == '': return "-"
    try: return pd.to_datetime(data).strftime('%d/%m/%Y')
    except: return str(data)

# --- CONEXÃO ---
def conectar_google_sheets():
    try:
        creds_json = st.secrets["credenciais_google"]
        creds_dict = json.loads(creds_json)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client.open("Banco de Dados CRM")
    except Exception as e:
        st.error(f"⚠️ Erro de Conexão: {e}")
        return None

# --- CARREGAMENTO ---
@st.cache_data(ttl=60)
def carregar_dados_completos():
    spreadsheet = conectar_google_sheets()
    if spreadsheet is None: return None, None, None
    try:
        # Config
        try:
            sheet_config = spreadsheet.worksheet("Config_Equipe")
            df_config = pd.DataFrame(sheet_config.get_all_records())
            for col in df_config.columns: df_config[col] = df_config[col].astype(str)
            
            # Tratamento das Metas
            if 'Meta_Fat' in df_config.columns: df_config['Meta_Fat'] = df_config['Meta_Fat'].apply(limpar_valor_inteiro)
            else: df_config['Meta_Fat'] = 0
            if 'Meta_Clientes' in df_config.columns: df_config['Meta_Clientes'] = df_config['Meta_Clientes'].apply(limpar_valor_inteiro)
            else: df_config['Meta_Clientes'] = 0
            if 'Meta_Atividades' in df_config.columns: df_config['Meta_Atividades'] = df_config['Meta_Atividades'].apply(limpar_valor_inteiro)
            else: df_config['Meta_Atividades'] = 0
        except: return None, None, None

        # Clientes
        try:
            sheet_clientes = spreadsheet.worksheet("Clientes")
            df_protheus = pd.DataFrame(sheet_clientes.get_all_records())
        except: return None, None, None
        
        # Leads
        try:
            sheet_leads = spreadsheet.worksheet("Novos_Leads")
            dados_leads = sheet_leads.get_all_records()
            df_leads = pd.DataFrame(dados_leads)
        except: df_leads = pd.DataFrame() 
            
        if not df_leads.empty:
            df_leads = df_leads.astype(str)
            df_protheus = df_protheus.astype(str)
            df_clientes = pd.concat([df_protheus, df_leads], ignore_index=True)
        else: df_clientes = df_protheus

        if not df_clientes.empty:
            df_clientes.columns = df_clientes.columns.str.strip()
            df_clientes['ID_Cliente_CNPJ_CPF'] = df_clientes['ID_Cliente_CNPJ_CPF'].astype(str)
            if 'Ultimo_Vendedor' in df_clientes.columns:
                df_clientes['Ultimo_Vendedor'] = df_clientes['Ultimo_Vendedor'].astype(str).str.strip()
            if 'Total_Compras' in df_clientes.columns:
                df_clientes['Total_Compras'] = df_clientes['Total_Compras'].apply(limpar_valor_inteiro)
            if 'Data_Ultima_Compra' in df_clientes.columns:
                df_clientes['Data_Ultima_Compra'] = pd.to_datetime(df_clientes['Data_Ultima_Compra'], dayfirst=True, errors='coerce')
        
        # Interações
        try:
            sheet_interacoes = spreadsheet.worksheet("Interacoes")
            df_interacoes = pd.DataFrame(sheet_interacoes.get_all_records())
            if not df_interacoes.empty:
                if 'Valor_Proposta' in df_interacoes.columns:
                    df_interacoes['Valor_Proposta'] = df_interacoes['Valor_Proposta'].apply(limpar_valor_inteiro)
                
                if 'Data' in df_interacoes.columns:
                    df_interacoes['Data_Obj'] = pd.to_datetime(df_interacoes['Data'], dayfirst=True, errors='coerce').dt.date
                if 'CNPJ_Cliente' in df_interacoes.columns:
                    mapa_nomes = dict(zip(df_clientes['ID_Cliente_CNPJ_CPF'].astype(str), df_clientes['Nome_Fantasia']))
                    df_interacoes['CNPJ_Cliente'] = df_interacoes['CNPJ_Cliente'].astype(str)
                    df_interacoes['Nome_Cliente'] = df_interacoes['CNPJ_Cliente'].map(mapa_nomes).fillna("Cliente Carteira")
        except:
            df_interacoes = pd.DataFrame(columns=['CNPJ_Cliente', 'Data', 'Tipo', 'Resumo', 'Vendedor', 'Valor_Proposta'])

        return df_clientes, df_interacoes, df_config
    except Exception as e:
        st.error(f"Erro Crítico: {e}")
        return None, None, None

# --- SALVAMENTO ---
def salvar_interacao_nuvem(cnpj, data_obj, tipo, resumo, vendedor, valor_inteiro):
    try:
        spreadsheet = conectar_google_sheets()
        sheet = spreadsheet.worksheet("Interacoes")
        data_str = data_obj.strftime('%d/%m/%Y')
        valor_save = int(valor_inteiro)
        
        id_prop = ""
        if tipo == "Orçamento Enviado":
            id_prop = f"#{gerar_id_proposta()}"
            resumo_final = f"{id_prop} {resumo}"
        else:
            resumo_final = resumo

        sheet.append_row([str(cnpj), data_str, tipo, resumo_final, vendedor, valor_save])
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro Salvar: {e}")
        return False

def salvar_novo_lead_completo(cnpj, nome, contato, telefone, vendedor, origem, primeira_acao, resumo_inicial, valor_inteiro):
    try:
        spreadsheet = conectar_google_sheets()
        sheet_leads = spreadsheet.worksheet("Novos_Leads")
        nova_linha = [str(cnpj), nome.upper(), contato, "NOVO LEAD", telefone, "", "", "0", "", "0", "", vendedor, origem]
        sheet_leads.append_row(nova_linha)
        sheet_interacoes = spreadsheet.worksheet("Interacoes")
        data_str = datetime.now().strftime('%d/%m/%Y')
        valor_save = int(valor_inteiro)
        
        id_prop = f"#{gerar_id_proposta()}" if primeira_acao == "Orçamento Enviado" else ""
        resumo_final = f"{id_prop} {resumo_inicial}" if id_prop else resumo_inicial
        
        sheet_interacoes.append_row([str(cnpj), data_str, primeira_acao, resumo_final, vendedor, valor_save])
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro Lead: {e}")
        return False

# --- IMPORTAÇÃO PROTHEUS ---
def processar_arquivo_protheus(uploaded_file, df_existente):
    try:
        df_import = pd.read_excel(uploaded_file)
        cols_necessarias = ['DATA', 'CNPJ', 'VENDEDOR', 'VALOR', 'PEDIDO', 'STATUS']
        if not all(col in df_import.columns for col in cols_necessarias):
            return False, f"Arquivo inválido! Colunas necessárias: {cols_necessarias}"
        
        novas_linhas = []
        contador_novos = 0
        contador_atualizados = 0
        pulos = 0
        
        estado_crm = {}
        if not df_existente.empty:
            df_sorted = df_existente.sort_values('Data_Obj', ascending=True)
            for _, row in df_sorted.iterrows():
                ped_id = extrair_pedido_protheus(row['Resumo'])
                if ped_id:
                    estado_crm[str(ped_id)] = {'valor': row['Valor_Proposta'], 'tipo': row['Tipo']}

        spreadsheet = conectar_google_sheets()
        sheet = spreadsheet.worksheet("Interacoes")
        
        for index, row in df_import.iterrows():
            pedido_id = str(row['PEDIDO']).strip()
            valor_novo = limpar_valor_inteiro(row['VALOR'])
            status_orig = str(row['STATUS']).upper().strip()
            
            tipo_novo = "Orçamento Enviado"
            if "FECHADO" in status_orig or "FATURADO" in status_orig: tipo_novo = "Venda Fechada"
            elif "CANCELADO" in status_orig: tipo_novo = "Venda Perdida"
                
            acao_necessaria = False
            motivo_atualizacao = ""
            
            if pedido_id not in estado_crm:
                acao_necessaria = True
                contador_novos += 1
            else:
                dados_antigos = estado_crm[pedido_id]
                val_antigo = dados_antigos['valor']
                tipo_antigo = dados_antigos['tipo']
                if abs(valor_novo - val_antigo) > 1 and tipo_novo == "Orçamento Enviado" and tipo_antigo == "Orçamento Enviado":
                    acao_necessaria = True
                    motivo_atualizacao = f"Atualização de Valor (Era {val_antigo})"
                    contador_atualizados += 1
                if tipo_novo != "Orçamento Enviado" and tipo_antigo == "Orçamento Enviado":
                    acao_necessaria = True
                    motivo_atualizacao = "Baixa Automática Protheus"
                    contador_atualizados += 1
            
            if acao_necessaria:
                cnpj = ''.join(filter(str.isdigit, str(row['CNPJ'])))
                try: data_obj = pd.to_datetime(row['DATA']).strftime('%d/%m/%Y')
                except: data_obj = datetime.now().strftime('%d/%m/%Y')
                crm_id_tag = f"#{gerar_id_proposta()} " if tipo_novo == "Orçamento Enviado" and pedido_id not in estado_crm else ""
                resumo_txt = f"{crm_id_tag}[PROTHEUS] Pedido: {pedido_id} | {status_orig}"
                if motivo_atualizacao: resumo_txt += f" | {motivo_atualizacao}"
                novas_linhas.append([cnpj, data_obj, tipo_novo, resumo_txt, str(row['VENDEDOR']).upper().strip(), valor_novo])
            else: pulos += 1
            
        if novas_linhas:
            sheet.append_rows(novas_linhas)
            st.cache_data.clear()
            return True, f"Importação: {contador_novos} Novos | {contador_atualizados} Atualizados | {pulos} Sem Mudança."
        else: return True, f"Tudo sincronizado. Nenhuma alteração encontrada."
    except Exception as e: return False, f"Erro no processamento: {e}"

# --- CALLBACKS ---
def processar_salvamento_lead(usuario_logado):
    nome = st.session_state["novo_nome"]
    doc = st.session_state["novo_doc"]
    val = st.session_state["novo_val"]
    if not nome or not doc: st.error("Campos obrigatórios!")
    else:
        if salvar_novo_lead_completo(doc, nome, st.session_state["novo_contato"], st.session_state["novo_tel"], usuario_logado, st.session_state["novo_origem"], st.session_state["novo_acao"], st.session_state["novo_resumo"], val):
            st.success("Salvo!")
            st.session_state["novo_nome"] = ""
            st.session_state["novo_doc"] = ""
            st.session_state["novo_val"] = 0

def processar_salvamento_vendedor(cid, usuario_logado, tipo_selecionado):
    obs = st.session_state["obs_temp"]
    val_raw = st.session_state.get("val_temp", 0)
    try: val = int(val_raw)
    except: val = 0
    if salvar_interacao_nuvem(cid, datetime.now(), tipo_selecionado, obs, usuario_logado, val):
        st.success("Salvo!")
        st.session_state["obs_temp"] = ""
        st.session_state["val_temp"] = 0

def fechar_proposta_automatica(cid, usuario_logado, proposta_row, status_novo):
    valor = proposta_row['Valor_Proposta']
    resumo_orig = proposta_row['Resumo']
    id_ref = extrair_id(resumo_orig)
    obs_id = id_ref if id_ref else "(S/ ID)"
    obs = f"Ref. Proposta {obs_id}. Detalhes: {resumo_orig}"
    if salvar_interacao_nuvem(cid, datetime.now(), status_novo, obs, usuario_logado, valor):
        st.success(f"Proposta {status_novo}!")
        time.sleep(1)
        st.rerun()

# --- APP ---
try:
    if URL_LOGO: st.sidebar.image(URL_LOGO, width=150)
    st.sidebar.title("🚀 CRM Master 21.0")
    
    with st.spinner("Conectando..."):
        df, df_interacoes, df_config = carregar_dados_completos()

    if df is not None and not df_config.empty:
        # LOGIN
        usuarios_validos = sorted(df_config['Usuario'].unique().tolist())
        if 'logado' not in st.session_state: st.session_state['logado'] = False
        
        if not st.session_state['logado']:
            st.sidebar.markdown("### 🔒 Acesso Restrito")
            usuario_input = st.sidebar.selectbox("Usuário:", usuarios_validos)
            senha_input = st.sidebar.text_input("Senha:", type="password")
            if st.sidebar.button("Entrar"):
                user_data = df_config[df_config['Usuario'] == usuario_input].iloc[0]
                if str(user_data['Senha']).strip() == str(senha_input).strip():
                    st.session_state['logado'] = True
                    st.session_state['usuario_atual'] = usuario_input
                    st.rerun()
                else: st.sidebar.error("Senha incorreta!")
            st.stop()
        
        usuario_logado = st.session_state['usuario_atual']
        if st.sidebar.button(f"Sair ({usuario_logado})"):
            st.session_state['logado'] = False
            st.rerun()

        user_data = df_config[df_config['Usuario'] == usuario_logado].iloc[0]
        tipo_usuario = str(user_data['Tipo']).upper().strip()
        carteiras_permitidas = [x.strip() for x in str(user_data['Carteira_Alvo']).split(',')]

        # --- PROCESSAMENTO GERAL ---
        hoje = datetime.now().date()
        if 'Data_Ultima_Compra' in df.columns: df['Dias_Sem_Comprar'] = (pd.Timestamp(hoje) - df['Data_Ultima_Compra']).dt.days
        else: df['Dias_Sem_Comprar'] = 0

        ids_resolvidos = []
        pedidos_baixados_protheus = set()
        if not df_interacoes.empty and 'Resumo' in df_interacoes.columns:
            resolvidos = df_interacoes[df_interacoes['Tipo'].isin(['Venda Fechada', 'Venda Perdida'])]
            for texto in resolvidos['Resumo'].astype(str):
                id_enc = extrair_id(texto)
                if id_enc: ids_resolvidos.append(id_enc)
                ped_enc = extrair_pedido_protheus(texto)
                if ped_enc: pedidos_baixados_protheus.add(ped_enc)

        def calcular_status(linha):
            cnpj = linha['ID_Cliente_CNPJ_CPF']
            cnpj_str = str(cnpj)
            status_calc = '🟢 ATIVO'
            if pd.isna(linha['Dias_Sem_Comprar']): status_calc = '🆕 NOVO S/ INTERAÇÃO'
            elif linha['Dias_Sem_Comprar'] >= 60: status_calc = '🔴 RECUPERAR'

            if not df_interacoes.empty and 'CNPJ_Cliente' in df_interacoes.columns:
                filtro = df_interacoes[df_interacoes['CNPJ_Cliente'] == cnpj_str]
                if not filtro.empty:
                    tem_proposta_aberta = False
                    orcamentos = filtro[filtro['Tipo'] == 'Orçamento Enviado']
                    for _, row in orcamentos.iterrows():
                        meu_id = extrair_id(row['Resumo'])
                        meu_pedido = extrair_pedido_protheus(row['Resumo'])
                        esta_resolvido = False
                        if meu_id and meu_id in ids_resolvidos: esta_resolvido = True
                        if meu_pedido and meu_pedido in pedidos_baixados_protheus: esta_resolvido = True
                        if not esta_resolvido: tem_proposta_aberta = True
                    
                    if tem_proposta_aberta: return '⏳ NEGOCIAÇÃO'
                    else:
                        filtro_sorted = filtro.sort_values('Data_Obj', ascending=True)
                        ultima = filtro_sorted.iloc[-1]
                        try:
                            if pd.notna(ultima['Data_Obj']):
                                if ultima['Tipo'] == 'Venda Fechada': return '⭐ VENDA RECENTE'
                                elif ultima['Tipo'] == 'Venda Perdida': return '👎 VENDA PERDIDA'
                                elif ultima['Tipo'] == 'Ligação Realizada': return '📞 CONTATADO RECENTEMENTE'
                                elif ultima['Tipo'] == 'WhatsApp Enviado': return '💬 WHATSAPP INICIADO'
                                elif ultima['Tipo'] == 'Orçamento Enviado': return '⏳ NEGOCIAÇÃO'
                        except: pass
            return status_calc
        df['Status'] = df.apply(calcular_status, axis=1)

        # --- CÁLCULO DE METAS (TRIPÉ) ---
        primeiro_dia_mes = hoje.replace(day=1)
        
        # 1. Metas do Usuário
        meta_fat = int(user_data.get('Meta_Fat', 0))
        meta_cli = int(user_data.get('Meta_Clientes', 0))
        meta_ativ = int(user_data.get('Meta_Atividades', 0))
        
        # 2. Reais do Usuário (Mês Atual)
        real_fat = 0
        real_cli = 0
        real_ativ = 0
        
        if not df_interacoes.empty:
            # Filtro Mes Atual + Usuario
            mask_mes = (df_interacoes['Vendedor'] == usuario_logado) & (df_interacoes['Data_Obj'] >= primeiro_dia_mes)
            df_mes = df_interacoes[mask_mes]
            
            if not df_mes.empty:
                # Faturamento
                real_fat = int(df_mes[df_mes['Tipo'] == 'Venda Fechada']['Valor_Proposta'].sum())
                
                # Clientes (Distintos)
                real_cli = df_mes[df_mes['Tipo'] == 'Venda Fechada']['CNPJ_Cliente'].nunique()
                
                # Atividades (Ligação, Zap, Visita)
                tipos_esforco = ['Ligação Realizada', 'WhatsApp Enviado', 'Agendou Visita']
                real_ativ = len(df_mes[df_mes['Tipo'].isin(tipos_esforco)])

        # 3. Exibição na Sidebar
        st.sidebar.markdown("### 🎯 Performance (Mês)")
        
        # Faturamento
        pct_fat = min(real_fat / meta_fat, 1.0) if meta_fat > 0 else 0
        st.sidebar.caption(f"💰 Fat: {formatar_moeda_visual(real_fat)} / {formatar_moeda_visual(meta_fat)}")
        st.sidebar.progress(pct_fat)
        
        # Clientes
        pct_cli = min(real_cli / meta_cli, 1.0) if meta_cli > 0 else 0
        st.sidebar.caption(f"👥 Clientes: {real_cli} / {meta_cli}")
        st.sidebar.progress(pct_cli)
        
        # Atividades
        pct_ativ = min(real_ativ / meta_ativ, 1.0) if meta_ativ > 0 else 0
        st.sidebar.caption(f"🔨 Atividades: {real_ativ} / {meta_ativ}")
        st.sidebar.progress(pct_ativ)

        # --- ÁREA DE IMPORTAÇÃO ---
        if tipo_usuario == "GESTOR":
            st.sidebar.markdown("---")
            with st.sidebar.expander("📥 Importar Protheus"):
                uploaded_file = st.file_uploader("Selecione .xlsx", type=["xlsx"])
                if uploaded_file and st.button("Processar"):
                    sucesso, msg = processar_arquivo_protheus(uploaded_file, df_interacoes)
                    if sucesso: st.success(msg)
                    else: st.error(msg)

        # CADASTRO
        if tipo_usuario == "VENDEDOR" or "TODOS" in carteiras_permitidas:
            st.sidebar.markdown("---")
            with st.sidebar.expander("➕ Cadastrar Lead"):
                if "novo_val" in st.session_state and isinstance(st.session_state["novo_val"], str): st.session_state["novo_val"] = 0
                for k in ["novo_nome", "novo_doc", "novo_contato", "novo_tel", "novo_resumo"]:
                    if k not in st.session_state: st.session_state[k] = ""
                if "novo_val" not in st.session_state: st.session_state["novo_val"] = 0
                if "novo_origem" not in st.session_state: st.session_state["novo_origem"] = "SELECIONE..."
                if "novo_acao" not in st.session_state: st.session_state["novo_acao"] = "SELECIONE..."
                st.text_input("Nome:", key="novo_nome")
                st.text_input("CPF/CNPJ:", key="novo_doc")
                c1, c2 = st.columns(2)
                c1.text_input("Contato:", key="novo_contato")
                c2.text_input("Telefone:", key="novo_tel")
                c3, c4 = st.columns(2)
                c3.selectbox("Origem:", ["SELECIONE...", "SZ.CHAT", "LIGAÇÃO", "PRESENCIAL", "E-MAIL", "INDICAÇÃO"], key="novo_origem")
                c4.selectbox("Ação:", ["SELECIONE...", "Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Agendou Visita"], key="novo_acao")
                if st.session_state["novo_acao"] == "Orçamento Enviado":
                    st.number_input("Valor (R$):", min_value=0, step=1, key="novo_val")
                st.text_area("Resumo:", key="novo_resumo")
                st.button("💾 SALVAR", type="primary", on_click=processar_salvamento_lead, args=(usuario_logado,))

        # FILTROS
        if "TODOS" in carteiras_permitidas:
            meus_clientes = df
            minhas_interacoes = df_interacoes
        else:
            if 'Ultimo_Vendedor' in df.columns: meus_clientes = df[df['Ultimo_Vendedor'].isin(carteiras_permitidas)]
            else: meus_clientes = pd.DataFrame()
            if not df_interacoes.empty and 'Vendedor' in df_interacoes.columns:
                minhas_interacoes = df_interacoes[df_interacoes['Vendedor'].isin(carteiras_permitidas)]
            else: minhas_interacoes = pd.DataFrame()

        # VIEW GESTOR
        if tipo_usuario == "GESTOR":
            st.divider()
            st.title(f"📊 Painel Geral")
            with st.container(border=True):
                col_f1, col_f2, col_f3, col_f4 = st.columns(4)
                d_ini = col_f1.date_input("De:", value=hoje - timedelta(days=30), format="DD/MM/YYYY")
                d_fim = col_f2.date_input("Até:", value=hoje, format="DD/MM/YYYY")
                opcoes_tipo = minhas_interacoes['Tipo'].unique().tolist() if not minhas_interacoes.empty else []
                tipos_sel = col_f3.multiselect("Tipos:", options=opcoes_tipo, default=opcoes_tipo)
                opcoes_vendedores = minhas_interacoes['Vendedor'].unique().tolist() if not minhas_interacoes.empty else []
                sel_vendedores = col_f4.multiselect("Vendedores:", options=opcoes_vendedores, default=opcoes_vendedores)

            vlr_orcado = vlr_fechado = vlr_perdido = vlr_pipeline = qtd_interacoes = 0
            
            if not minhas_interacoes.empty and 'Data_Obj' in minhas_interacoes.columns:
                mask_data = ((minhas_interacoes['Data_Obj'] >= d_ini) & (minhas_interacoes['Data_Obj'] <= d_fim))
                if sel_vendedores: mask_data = mask_data & (minhas_interacoes['Vendedor'].isin(sel_vendedores))
                df_filtered = minhas_interacoes[mask_data].copy()
                
                vlr_orcado = int(df_filtered[df_filtered['Tipo'] == 'Orçamento Enviado']['Valor_Proposta'].sum())
                vlr_perdido = int(df_filtered[df_filtered['Tipo'] == 'Venda Perdida']['Valor_Proposta'].sum())
                vlr_fechado = int(df_filtered[df_filtered['Tipo'] == 'Venda Fechada']['Valor_Proposta'].sum())
                qtd_interacoes = len(df_filtered)
                
                orcamentos_periodo = df_filtered[df_filtered['Tipo'] == 'Orçamento Enviado']
                mapa_ultimos_valores = {}
                for _, row in orcamentos_periodo.iterrows():
                    pid = extrair_id(row['Resumo'])
                    ped_proth = extrair_pedido_protheus(row['Resumo'])
                    chave_unica = ped_proth if ped_proth else pid
                    if chave_unica:
                        esta_baixado = False
                        if pid and pid in ids_resolvidos: esta_baixado = True
                        if ped_proth and ped_proth in pedidos_baixados_protheus: esta_baixado = True
                        if not esta_baixado: mapa_ultimos_valores[chave_unica] = row['Valor_Proposta']
                vlr_pipeline = sum(mapa_ultimos_valores.values())
                
                if tipos_sel: df_tabela = df_filtered[df_filtered['Tipo'].isin(tipos_sel)]
                else: df_tabela = df_filtered
            else:
                df_filtered = pd.DataFrame()
                df_tabela = pd.DataFrame()

            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("💰 Orçado", formatar_moeda_visual(vlr_orcado))
            k2.metric("🔮 Na Mesa", formatar_moeda_visual(vlr_pipeline))
            k3.metric("✅ Fechado", formatar_moeda_visual(vlr_fechado))
            k4.metric("👎 Perdido", formatar_moeda_visual(vlr_perdido))
            k5.metric("📞 Interações", f"{qtd_interacoes}")
            
            st.divider()
            t1, t2 = st.tabs(["🏆 Ranking de Performance", "📝 Detalhes"])
            with t1:
                if not df_filtered.empty:
                    # Preparando dados para Ranking Complexo
                    df_filtered['Is_Orcamento'] = (df_filtered['Tipo'] == 'Orçamento Enviado').astype(int)
                    df_filtered['Is_Fechado'] = (df_filtered['Tipo'] == 'Venda Fechada').astype(int)
                    df_filtered['Is_Atividade'] = df_filtered['Tipo'].isin(['Ligação Realizada', 'WhatsApp Enviado', 'Agendou Visita']).astype(int)
                    df_filtered['Valor_Venda'] = df_filtered.apply(lambda x: x['Valor_Proposta'] if x['Tipo'] == 'Venda Fechada' else 0, axis=1)
                    
                    ranking = df_filtered.groupby('Vendedor').agg(
                        Fat_Real=('Valor_Venda', 'sum'),
                        Cli_Real=('CNPJ_Cliente', lambda x: x[df_filtered.loc[x.index, 'Tipo'] == 'Venda Fechada'].nunique()),
                        Ativ_Real=('Is_Atividade', 'sum')
                    ).reset_index()

                    # Merge com Metas
                    df_metas = df_config[['Usuario', 'Meta_Fat', 'Meta_Clientes', 'Meta_Atividades']].rename(columns={'Usuario': 'Vendedor'})
                    ranking = pd.merge(ranking, df_metas, on='Vendedor', how='left').fillna(0)
                    
                    # Colunas Visuais
                    ranking['R$ Faturamento'] = ranking['Fat_Real'].apply(formatar_moeda_visual)
                    ranking['% Meta Fat'] = ranking.apply(lambda x: f"{(x['Fat_Real']/x['Meta_Fat']*100):.0f}%" if x['Meta_Fat']>0 else "-", axis=1)
                    
                    ranking['Qtd Clientes'] = ranking['Cli_Real'].astype(int)
                    ranking['% Meta Cli'] = ranking.apply(lambda x: f"{(x['Cli_Real']/x['Meta_Clientes']*100):.0f}%" if x['Meta_Clientes']>0 else "-", axis=1)
                    
                    ranking['Qtd Atividades'] = ranking['Ativ_Real'].astype(int)
                    ranking['% Meta Ativ'] = ranking.apply(lambda x: f"{(x['Ativ_Real']/x['Meta_Atividades']*100):.0f}%" if x['Meta_Atividades']>0 else "-", axis=1)
                    
                    ranking_view = ranking[['Vendedor', 'R$ Faturamento', '% Meta Fat', 'Qtd Clientes', '% Meta Cli', 'Qtd Atividades', '% Meta Ativ']]
                    st.dataframe(ranking_view, use_container_width=True)
                else: st.info("Sem dados.")
            with t2:
                if not df_tabela.empty:
                    view = df_tabela[['Data_Obj', 'Nome_Cliente', 'Tipo', 'Resumo', 'Valor_Proposta', 'Vendedor']]
                    view['Valor_Proposta'] = view['Valor_Proposta'].apply(formatar_moeda_visual)
                    st.dataframe(view, use_container_width=True, hide_index=True)
                else: st.info("Nenhuma interação.")

        # VIEW VENDEDOR
        else:
            st.divider()
            st.title(f"💼 Minha Carteira")
            if meus_clientes.empty: st.warning("Nenhum cliente atribuído.")
            else:
                c_esq, c_dir = st.columns([1, 1])
                with c_esq:
                    st.subheader("Clientes")
                    termo_busca = st.text_input("🔍 Buscar:", placeholder="Nome ou CPF/CNPJ...")
                    if termo_busca:
                        termo_busca = termo_busca.upper()
                        lista = meus_clientes[meus_clientes['Nome_Fantasia'].str.upper().str.contains(termo_busca, na=False) | meus_clientes['ID_Cliente_CNPJ_CPF'].astype(str).str.contains(termo_busca, na=False)]
                        if lista.empty: st.warning("Não encontrado.")
                    else:
                        ops = ['🔴 RECUPERAR', '⚠️ FOLLOW-UP', '⏳ NEGOCIAÇÃO', '💬 WHATSAPP INICIADO', '👎 VENDA PERDIDA', '⭐ VENDA RECENTE', '🟢 ATIVO']
                        sel_status = st.multiselect("Status:", ops, default=['⚠️ FOLLOW-UP', '⏳ NEGOCIAÇÃO'])
                        lista = meus_clientes[meus_clientes['Status'].isin(sel_status)].sort_values('Status', ascending=False)
                        if lista.empty: st.info("Filtro vazio.")
                    cid = None
                    if not lista.empty:
                        with st.container(height=600):
                            cid = st.radio("Selecione:", lista['ID_Cliente_CNPJ_CPF'].tolist(), format_func=lambda x: f"{formatar_documento(x)} | {lista[lista['ID_Cliente_CNPJ_CPF']==x]['Nome_Fantasia'].values[0]}")
                with c_dir:
                    if cid and not meus_clientes[meus_clientes['ID_Cliente_CNPJ_CPF'] == cid].empty:
                        cli = meus_clientes[meus_clientes['ID_Cliente_CNPJ_CPF'] == cid].iloc[0]
                        with st.container(border=True):
                            st.markdown(f"### {cli['Nome_Fantasia']}")
                            col_d1, col_d2 = st.columns(2)
                            
                            # DADOS DE CONTATO E LOCAL
                            v_contato = cli.get('Nome_Contato', '-') if 'Nome_Contato' in cli else cli.get('Contato', '-')
                            col_d1.write(f"**👤 Contato:** {v_contato}")
                            col_d1.write(f"**📞 Tel:** {cli.get('Telefone_Contato1', '-')}")
                            # Novos Campos (Cidade/UF)
                            cidade = cli.get('Cidade', '-')
                            uf = cli.get('UF', '-')
                            col_d1.write(f"**📍 Local:** {cidade}/{uf}")

                            # DADOS COMERCIAIS
                            v_dono = cli.get('Ultimo_Vendedor', '-')
                            col_d2.write(f"**👔 Carteira:** {v_dono}")
                            col_d2.write(f"**💰 Total:** {formatar_moeda_visual(cli.get('Total_Compras', 0))}")
                            # Novo Campo (Data Ultima Compra)
                            dt_ult = formatar_data_br(cli.get('Data_Ultima_Compra', '-'))
                            col_d2.write(f"**📅 Últ. Compra:** {dt_ult}")

                            st.divider()
                            tab_hist, tab_prop, tab_nova = st.tabs(["📜 Histórico", "💰 Propostas Abertas", "📝 Nova Ação"])
                            with tab_hist:
                                if not df_interacoes.empty and 'CNPJ_Cliente' in df_interacoes.columns:
                                    hist = df_interacoes[df_interacoes['CNPJ_Cliente'] == str(cid)]
                                    if not hist.empty:
                                        hist_view = hist.iloc[::-1][['Data_Obj', 'Tipo', 'Resumo', 'Valor_Proposta']].head(10)
                                        hist_view.rename(columns={'Data_Obj': 'Data', 'Valor_Proposta': 'Valor'}, inplace=True)
                                        hist_view['Valor'] = hist_view['Valor'].apply(formatar_moeda_visual)
                                        hist_view['Data'] = hist_view['Data'].apply(formatar_data_br)
                                        st.dataframe(hist_view, hide_index=True, use_container_width=True)
                                    else: st.write("Nada aqui.")
                            with tab_prop:
                                if not df_interacoes.empty and 'CNPJ_Cliente' in df_interacoes.columns:
                                    props = df_interacoes[(df_interacoes['CNPJ_Cliente'] == str(cid)) & (df_interacoes['Tipo'] == 'Orçamento Enviado')].copy()
                                    props_ativas = []
                                    if not props.empty:
                                        props = props.iloc[::-1]
                                        for _, row in props.iterrows():
                                            pid = extrair_id(row['Resumo'])
                                            ped_proth = extrair_pedido_protheus(row['Resumo'])
                                            esta_resolvido = False
                                            if pid and pid in ids_resolvidos: esta_resolvido = True
                                            if ped_proth and ped_proth in pedidos_baixados_protheus: esta_resolvido = True
                                            if not esta_resolvido: props_ativas.append(row)
                                    if props_ativas:
                                        for idx, row in enumerate(props_ativas):
                                            with st.container(border=True):
                                                c_p1, c_p2, c_p3 = st.columns([2, 1, 1])
                                                dt_p = formatar_data_br(row['Data_Obj'])
                                                val_p = formatar_moeda_visual(row['Valor_Proposta'])
                                                resumo_p = row['Resumo']
                                                c_p1.markdown(f"**{dt_p}** | {val_p}")
                                                c_p1.caption(f"{resumo_p}")
                                                if c_p2.button("✅ FECHAR", key=f"btn_win_{idx}_{row['Resumo']}"):
                                                    fechar_proposta_automatica(cid, usuario_logado, row, "Venda Fechada")
                                                if c_p3.button("❌ PERDER", key=f"btn_loss_{idx}_{row['Resumo']}"):
                                                    fechar_proposta_automatica(cid, usuario_logado, row, "Venda Perdida")
                                    else: st.info("Nenhuma proposta em aberto.")
                            with tab_nova:
                                tipo = st.selectbox("Ação:", ["Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Agendou Visita"])
                                if tipo == "Orçamento Enviado":
                                    if "val_temp" in st.session_state and isinstance(st.session_state["val_temp"], str): st.session_state["val_temp"] = 0
                                    st.number_input("Valor (R$):", min_value=0, step=1, key="val_temp")
                                else:
                                    if "val_temp" not in st.session_state: st.session_state["val_temp"] = 0
                                st.text_area("Obs:", key="obs_temp")
                                st.button("✅ Salvar", type="primary", on_click=processar_salvamento_vendedor, args=(cid, usuario_logado, tipo))

except Exception as e:
    st.error(f"Erro Fatal: {e}")
