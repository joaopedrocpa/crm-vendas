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
import numpy as np

# --- 1. CONFIGURAÇÃO VISUAL (Primeira linha obrigatória) ---
st.set_page_config(page_title="CRM Master 24.0", layout="wide")
URL_LOGO = "https://cdn-icons-png.flaticon.com/512/9187/9187604.png"

# --- CSS (Visual Dark & Scroll) ---
st.markdown("""
<style>
    [data-testid="stSidebar"] {min-width: 300px;}
    div[data-testid="stMetric"] {background-color: #262730; border: 1px solid #464b5c; padding: 10px; border-radius: 5px;}
    .stButton button {width: 100%; font-weight: bold;}
    .stProgress > div > div > div > div {background-color: #00ff00;}
    /* Ajuste para Radio Button com Scroll */
    div.row-widget.stRadio > div {flex-direction: column;}
</style>
""", unsafe_allow_html=True)

# --- 2. FUNÇÕES AUXILIARES (Helpers) ---
def gerar_id_proposta(): return ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))

def extrair_id(t): 
    match = re.search(r'(#[A-Z0-9]{4})', str(t))
    return match.group(1) if pd.notna(t) and match else None

def extrair_pedido_protheus(t): 
    match = re.search(r'\[PROTHEUS\] Pedido: (\w+)', str(t))
    return match.group(1) if pd.notna(t) and match else None

def limpar_int(v): 
    try: return int(re.sub(r'[^\d]', '', str(v).split(',')[0])) if pd.notna(v) and str(v).strip() else 0
    except: return 0

def fmt_moeda(v): 
    try: return f"R$ {int(v):,.0f}".replace(',', '.')
    except: return "R$ 0"

def fmt_data(d): 
    return pd.to_datetime(d).strftime('%d/%m/%Y') if pd.notna(d) and str(d).strip() != '' else "-"

def fmt_doc(v):
    d = ''.join(filter(str.isdigit, str(v)))
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}" if len(d)>11 else f"{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}"

# --- 3. CONEXÃO GOOGLE ---
def conectar_google_sheets():
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(st.secrets["credenciais_google"]), ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds).open("Banco de Dados CRM")
    except Exception as e:
        st.error(f"Erro Conexão: {e}")
        return None

# --- 4. CARREGAMENTO COM CACHE (Velocidade) ---
@st.cache_data(ttl=3600)
def carregar_dados_cache():
    ss = conectar_google_sheets()
    if not ss: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Config
    try:
        df_cfg = pd.DataFrame(ss.worksheet("Config_Equipe").get_all_records()).astype(str)
        for c in ['Meta_Fat','Meta_Clientes','Meta_Atividades']: 
            if c in df_cfg.columns: df_cfg[c] = df_cfg[c].apply(limpar_int)
    except: df_cfg = pd.DataFrame()

    # Clientes
    try:
        df_cli = pd.DataFrame(ss.worksheet("Clientes").get_all_records())
        if not df_cli.empty:
            df_cli.columns = df_cli.columns.str.strip()
            df_cli['ID_Cliente_CNPJ_CPF'] = df_cli['ID_Cliente_CNPJ_CPF'].astype(str)
            if 'Total_Compras' in df_cli.columns: df_cli['Total_Compras'] = df_cli['Total_Compras'].apply(limpar_int)
            if 'Data_Ultima_Compra' in df_cli.columns: df_cli['Data_Ultima_Compra'] = pd.to_datetime(df_cli['Data_Ultima_Compra'], dayfirst=True, errors='coerce')
    except: df_cli = pd.DataFrame()

    # Leads
    try:
        df_leads = pd.DataFrame(ss.worksheet("Novos_Leads").get_all_records()).astype(str)
        if not df_leads.empty: df_cli = pd.concat([df_cli, df_leads], ignore_index=True)
    except: pass

    # Interações
    try:
        df_int = pd.DataFrame(ss.worksheet("Interacoes").get_all_records())
        if not df_int.empty:
            if 'Valor_Proposta' in df_int.columns: df_int['Valor_Proposta'] = df_int['Valor_Proposta'].apply(limpar_int)
            if 'Data' in df_int.columns: df_int['Data_Obj'] = pd.to_datetime(df_int['Data'], dayfirst=True, errors='coerce').dt.date
            df_int['CNPJ_Cliente'] = df_int['CNPJ_Cliente'].astype(str)
            
            # Mapeamento Rápido de Nomes
            if 'Nome_Cliente' not in df_int.columns: df_int['Nome_Cliente'] = None
            mapa = dict(zip(df_cli['ID_Cliente_CNPJ_CPF'], df_cli['Nome_Fantasia']))
            mask_n = df_int['Nome_Cliente'].isna() | (df_int['Nome_Cliente'] == "")
            df_int.loc[mask_n, 'Nome_Cliente'] = df_int.loc[mask_n, 'CNPJ_Cliente'].map(mapa).fillna("Cliente Carteira")
    except: df_int = pd.DataFrame(columns=['CNPJ_Cliente','Data','Tipo','Resumo','Vendedor','Valor_Proposta','Data_Obj','Nome_Cliente'])

    return df_cfg, df_cli, df_int

# --- 5. MOTOR DE CÁLCULO VETORIZADO (Rápido) ---
def recalcular_status_massa(df_c, df_i):
    if df_c.empty: return df_c
    
    hoje = datetime.now().date()
    df_c['Status'] = '🟢 ATIVO'
    
    # Dias sem Comprar
    if 'Data_Ultima_Compra' in df_c.columns:
        df_c['Dias_Sem_Comprar'] = (pd.Timestamp(hoje) - df_c['Data_Ultima_Compra']).dt.days
        df_c.loc[df_c['Dias_Sem_Comprar'] >= 60, 'Status'] = '🔴 RECUPERAR'
        df_c.loc[df_c['Dias_Sem_Comprar'].isna(), 'Status'] = '🆕 NOVO S/ INTERAÇÃO'
    
    if df_i.empty: return df_c

    # Identificar Negociações Abertas
    fechados = df_i[df_i['Tipo'].isin(['Venda Fechada', 'Venda Perdida'])]
    ids_baixados = set(fechados['Resumo'].apply(extrair_id).dropna())
    peds_baixados = set(fechados['Resumo'].apply(extrair_pedido_protheus).dropna())
    
    orcs = df_i[df_i['Tipo'] == 'Orçamento Enviado'].copy()
    orcs['ID_T'] = orcs['Resumo'].apply(extrair_id)
    orcs['Ped_T'] = orcs['Resumo'].apply(extrair_pedido_protheus)
    
    mask_aberto = ((~orcs['ID_T'].isin(ids_baixados) & orcs['ID_T'].notna()) | 
                   (~orcs['Ped_T'].isin(peds_baixados) & orcs['Ped_T'].notna()))
    cnpjs_neg = orcs[mask_aberto]['CNPJ_Cliente'].unique()
    
    df_c.loc[df_c['ID_Cliente_CNPJ_CPF'].isin(cnpjs_neg), 'Status'] = '⏳ NEGOCIAÇÃO'
    
    return df_c

# --- 6. FUNÇÕES DE SALVAMENTO ---
def salvar_nuvem(cnpj, data, tipo, resumo, vend, val):
    try:
        ss = conectar_google_sheets()
        ss.worksheet("Interacoes").append_row([str(cnpj), data.strftime('%d/%m/%Y'), tipo, resumo, vend, int(val)])
        
        # Atualiza Sessão Local
        novo = {'CNPJ_Cliente':str(cnpj),'Data_Obj':data,'Tipo':tipo,'Resumo':resumo,'Vendedor':vend,'Valor_Proposta':int(val),'Nome_Cliente':'...'}
        st.session_state['df_int'] = pd.concat([st.session_state['df_int'], pd.DataFrame([novo])], ignore_index=True)
        st.session_state['df_cli'] = recalcular_status_massa(st.session_state['df_cli'], st.session_state['df_int'])
        return True
    except: return False

def salvar_lead(nome, doc, cont, tel, vend, ori, acao, res, val):
    try:
        ss = conectar_google_sheets()
        ss.worksheet("Novos_Leads").append_row([str(doc), nome.upper(), cont, "NOVO LEAD", tel, "", "", "0", "", "0", "", vend, ori])
        st.cache_data.clear() 
        if acao:
            id_p = f"#{gerar_id_proposta()} " if acao == "Orçamento Enviado" else ""
            ss.worksheet("Interacoes").append_row([str(doc), datetime.now().strftime('%d/%m/%Y'), acao, f"{id_p}{res}", vend, int(val)])
        return True
    except: return False

def proc_import(file, df_old):
    try:
        df = pd.read_excel(file)
        if not {'DATA','CNPJ','VENDEDOR','VALOR','PEDIDO','STATUS'}.issubset(df.columns): return False, "Colunas Erradas"
        peds_ex = set(df_old['Resumo'].apply(extrair_pedido_protheus).dropna()) if not df_old.empty else set()
        novos = []
        
        for _, r in df.iterrows():
            pid = str(r['PEDIDO']).strip()
            if pid not in peds_ex:
                stt = str(r['STATUS']).upper()
                tipo = "Venda Fechada" if "FECHADO" in stt or "FATURADO" in stt else ("Venda Perdida" if "CANCELADO" in stt else "Orçamento Enviado")
                res = f"{'#'+gerar_id_proposta()+' ' if tipo=='Orçamento Enviado' else ''}[PROTHEUS] Pedido: {pid} | {stt}"
                try: dt = pd.to_datetime(r['DATA']).strftime('%d/%m/%Y')
                except: dt = datetime.now().strftime('%d/%m/%Y')
                novos.append([''.join(filter(str.isdigit, str(r['CNPJ']))), dt, tipo, res, str(r['VENDEDOR']).upper().strip(), limpar_int(r['VALOR'])])
        
        if novos: 
            conectar_google_sheets().worksheet("Interacoes").append_rows(novos)
            st.cache_data.clear()
            return True, f"{len(novos)} importados."
        return True, "Nada novo."
    except Exception as e: return False, str(e)

# --- 7. APP PRINCIPAL ---
# Inicialização de Estado Seguro
if 'logado' not in st.session_state: st.session_state['logado'] = False
if 'df_cli' not in st.session_state:
    cfg, cli, inter = carregar_dados_cache()
    if not cli.empty: cli = recalcular_status_massa(cli, inter)
    st.session_state.update({'df_cfg':cfg, 'df_cli':cli, 'df_int':inter})

df_cfg = st.session_state['df_cfg']

# --- TELA DE LOGIN (Bloqueante) ---
if not st.session_state['logado']:
    if URL_LOGO: st.sidebar.image(URL_LOGO, width=150)
    st.sidebar.title("CRM Login")
    
    # Verifica se config carregou
    if df_cfg.empty:
        st.error("Erro ao carregar configurações. Verifique o banco de dados.")
        st.stop()
        
    usrs = sorted(df_cfg['Usuario'].unique())
    u = st.sidebar.selectbox("Usuário", usrs)
    p = st.sidebar.text_input("Senha", type="password")
    
    if st.sidebar.button("Entrar"):
        ud = df_cfg[df_cfg['Usuario']==u].iloc[0]
        if str(ud['Senha']).strip() == str(p).strip():
            st.session_state['logado'] = True
            st.session_state['u_atual'] = u
            st.rerun()
        else:
            st.error("Senha Incorreta")
    st.stop() # PARA TUDO AQUI SE NÃO ESTIVER LOGADO

# --- DADOS DO USUÁRIO LOGADO ---
u_log = st.session_state['u_atual']
df_cli = st.session_state['df_cli']
df_int = st.session_state['df_int']

# Atualiza dados do usuário
u_data = df_cfg[df_cfg['Usuario']==u_log].iloc[0]
tipo_u = str(u_data['Tipo']).upper().strip()
carts = [x.strip() for x in str(u_data['Carteira_Alvo']).split(',')]

# --- SIDEBAR COMPLETA ---
if URL_LOGO: st.sidebar.image(URL_LOGO, width=150)
st.sidebar.title(f"Olá, {u_log}")

if st.sidebar.button("🔄 Atualizar Dados"):
    st.cache_data.clear()
    st.rerun()

if st.sidebar.button("Sair"):
    st.session_state['logado'] = False
    st.rerun()

st.sidebar.divider()

# Metas Sidebar
prim_dia = datetime.now().date().replace(day=1)
if not df_int.empty:
    df_m = df_int[(df_int['Vendedor']==u_log) & (df_int['Data_Obj']>=prim_dia)]
    fat_r = df_m[df_m['Tipo']=='Venda Fechada']['Valor_Proposta'].sum()
    cli_r = df_m[df_m['Tipo']=='Venda Fechada']['CNPJ_Cliente'].nunique()
    ativ_r = len(df_m[df_m['Tipo'].isin(['Ligação Realizada','WhatsApp Enviado','Agendou Visita'])])
else: fat_r=0; cli_r=0; ativ_r=0

mf, mc, ma = u_data.get('Meta_Fat',0), u_data.get('Meta_Clientes',0), u_data.get('Meta_Atividades',0)

st.sidebar.markdown("### 🎯 Metas Mês")
st.sidebar.caption(f"💰 Fat: {fmt_moeda(fat_r)} / {fmt_moeda(mf)}")
st.sidebar.progress(min(fat_r/mf, 1.0) if mf > 0 else 0)
st.sidebar.caption(f"👥 Cli: {cli_r} / {mc}")
st.sidebar.progress(min(cli_r/mc, 1.0) if mc > 0 else 0)
st.sidebar.caption(f"🔨 Ativ: {ativ_r} / {ma}")
st.sidebar.progress(min(ativ_r/ma, 1.0) if ma > 0 else 0)

# Ações Sidebar
st.sidebar.divider()
if tipo_u == "GESTOR":
    with st.sidebar.expander("📥 Importar Protheus"):
        f = st.file_uploader("Arquivo Excel", type=["xlsx"])
        if f and st.button("Processar"): 
            ok, msg = proc_import(f, df_int)
            if ok: st.success(msg); time.sleep(2); st.rerun()
            else: st.error(msg)

if "TODOS" in carts or tipo_u == "VENDEDOR":
    with st.sidebar.expander("➕ Novo Lead"):
        n = st.text_input("Nome", key="ln"); d = st.text_input("CPF/CNPJ", key="ld")
        c = st.text_input("Contato", key="lc"); t = st.text_input("Tel", key="lt")
        o = st.selectbox("Origem", ["LIGAÇÃO","E-MAIL","INDICAÇÃO"], key="lo")
        a = st.selectbox("Ação", ["Ligação Realizada","Orçamento Enviado"], key="la")
        v = st.number_input("R$", step=1, key="lv") if a == "Orçamento Enviado" else 0
        r = st.text_area("Resumo", key="lr")
        if st.button("Salvar Lead"):
            if salvar_lead(n,d,c,t,u_log,o,a,r,v): st.success("Lead Salvo!"); time.sleep(1); st.rerun()

# --- FILTRAGEM ---
meus_cli = df_cli if "TODOS" in carts else df_cli[df_cli['Ultimo_Vendedor'].isin(carts)]
minhas_int = df_int if "TODOS" in carts else df_int[df_int['Vendedor'].isin(carts)]

# --- VIEW GESTOR ---
if tipo_u == "GESTOR":
    st.title("📊 Painel Geral")
    with st.container(border=True):
        c1,c2,c3 = st.columns(3)
        di = c1.date_input("De", value=datetime.now()-timedelta(days=30))
        df = c2.date_input("Até", value=datetime.now())
        sel_v = c3.multiselect("Vendedores", minhas_int['Vendedor'].unique())
    
    if not minhas_int.empty:
        msk = (minhas_int['Data_Obj']>=di) & (minhas_int['Data_Obj']<=df)
        if sel_v: msk &= minhas_int['Vendedor'].isin(sel_v)
        dff = minhas_int[msk]
        
        # Pipeline Logic
        resols = set(dff[dff['Tipo'].isin(['Venda Fechada','Venda Perdida'])]['Resumo'])
        ids_res = set([extrair_id(x) for x in resols if extrair_id(x)])
        peds_res = set([extrair_pedido_protheus(x) for x in resols if extrair_pedido_protheus(x)])
        
        mesa = 0
        for _, row in dff[dff['Tipo']=='Orçamento Enviado'].iterrows():
            if not ((extrair_id(row['Resumo']) in ids_res) or (extrair_pedido_protheus(row['Resumo']) in peds_res)):
                mesa += row['Valor_Proposta']
        
        k1,k2,k3 = st.columns(3)
        k1.metric("Orçado", fmt_moeda(dff[dff['Tipo']=='Orçamento Enviado']['Valor_Proposta'].sum()))
        k2.metric("Na Mesa", fmt_moeda(mesa))
        k3.metric("Fechado", fmt_moeda(dff[dff['Tipo']=='Venda Fechada']['Valor_Proposta'].sum()))
        
        if not dff.empty:
            agg = dff.groupby('Vendedor').agg(
                Fat=('Valor_Proposta', lambda x: x[dff.loc[x.index,'Tipo']=='Venda Fechada'].sum()),
                Cli=('CNPJ_Cliente', lambda x: x[dff.loc[x.index,'Tipo']=='Venda Fechada'].nunique())
            ).reset_index()
            st.dataframe(agg, use_container_width=True)

# --- VIEW VENDEDOR (Com Scroll e Filtros Padrão) ---
else:
    st.title("💼 Minha Carteira")
    col_list, col_det = st.columns([1, 1.2]) # Coluna da direita um pouco maior
    
    with col_list:
        st.markdown("### 🔍 Filtros")
        busca = st.text_input("Buscar por Nome ou CNPJ", placeholder="Digite para filtrar...")
        
        # FILTROS PADRÃO VOLTARAM
        status_padrao = ['⏳ NEGOCIAÇÃO', '⚠️ FOLLOW-UP']
        filtro_status = st.multiselect("Status", ['🔴 RECUPERAR', '⚠️ FOLLOW-UP', '⏳ NEGOCIAÇÃO', '🟢 ATIVO'], default=status_padrao)
        
        # Lógica de Filtragem
        if busca:
            busca = busca.upper()
            lista_final = meus_cli[meus_cli['Nome_Fantasia'].str.upper().str.contains(busca, na=False) | meus_cli['ID_Cliente_CNPJ_CPF'].astype(str).str.contains(busca, na=False)]
        else:
            lista_final = meus_cli[meus_cli['Status'].isin(filtro_status)].sort_values('Status')

        st.caption(f"{len(lista_final)} clientes encontrados.")

        # ÁREA COM SCROLL (CONTAINER)
        cid_selecionado = None
        if not lista_final.empty:
            # Limita a 100 para não travar o navegador, mas com scroll
            with st.container(height=600): # Altura fixa gera a barra de rolagem
                cid_selecionado = st.radio(
                    "Selecione o Cliente:",
                    lista_final.head(100)['ID_Cliente_CNPJ_CPF'].tolist(),
                    format_func=lambda x: f"[{lista_final[lista_final['ID_Cliente_CNPJ_CPF']==x]['Status'].values[0]}] {lista_final[lista_final['ID_Cliente_CNPJ_CPF']==x]['Nome_Fantasia'].values[0]}"
                )
        else:
            st.info("Nenhum cliente com esses filtros.")

    with col_det:
        if cid_selecionado:
            # Pega dados do cliente selecionado com segurança
            c_dados = meus_cli[meus_cli['ID_Cliente_CNPJ_CPF'] == cid_selecionado].iloc[0]
            
            with st.container(border=True):
                st.subheader(c_dados['Nome_Fantasia'])
                st.caption(f"CNPJ: {fmt_doc(cid_selecionado)}")
                
                # Detalhes em Colunas
                d1, d2 = st.columns(2)
                d1.markdown(f"**👤 Contato:** {c_dados.get('Contato', c_dados.get('Nome_Contato', '-'))}")
                d1.markdown(f"**📞 Tel:** {c_dados.get('Telefone_Contato1', '-')}")
                d1.markdown(f"**📍 Local:** {c_dados.get('Cidade','-')}/{c_dados.get('UF','-')}")
                
                d2.markdown(f"**👔 Carteira:** {c_dados.get('Ultimo_Vendedor','-')}")
                d2.markdown(f"**💰 Total:** {fmt_moeda(c_dados.get('Total_Compras', 0))}")
                d2.markdown(f"**📅 Última:** {fmt_data(c_dados.get('Data_Ultima_Compra', '-'))}")
                
                st.divider()
                
                # Interações do Cliente
                c_ints = minhas_int[minhas_int['CNPJ_Cliente'] == str(cid_selecionado)].sort_values('Data_Obj', ascending=False)
                
                tab1, tab2, tab3 = st.tabs(["📜 Histórico", "💰 Propostas Abertas", "📝 Nova Ação"])
                
                with tab1:
                    if not c_ints.empty:
                        view = c_ints[['Data_Obj', 'Tipo', 'Resumo', 'Valor_Proposta']].copy()
                        view['Valor_Proposta'] = view['Valor_Proposta'].apply(fmt_moeda)
                        view['Data_Obj'] = view['Data_Obj'].apply(fmt_data)
                        st.dataframe(view, hide_index=True, use_container_width=True)
                    else: st.info("Sem histórico.")
                
                with tab2:
                    # Lógica de "Na Mesa" Local
                    resols_cli = set(c_ints[c_ints['Tipo'].isin(['Venda Fechada', 'Venda Perdida'])]['Resumo'])
                    ids_res = set([extrair_id(x) for x in resols_cli if extrair_id(x)])
                    peds_res = set([extrair_pedido_protheus(x) for x in resols_cli if extrair_pedido_protheus(x)])
                    
                    abertas = []
                    for _, row in c_ints[c_ints['Tipo'] == 'Orçamento Enviado'].iterrows():
                        pid, ped = extrair_id(row['Resumo']), extrair_pedido_protheus(row['Resumo'])
                        if not ((pid and pid in ids_res) or (ped and ped in peds_res)):
                            abertas.append(row)
                    
                    if abertas:
                        for i, r in enumerate(abertas):
                            with st.container(border=True):
                                ca, cb, cc = st.columns([3, 1, 1])
                                ca.markdown(f"**{fmt_data(r['Data_Obj'])}** | {fmt_moeda(r['Valor_Proposta'])}")
                                ca.caption(r['Resumo'])
                                if cb.button("✅", key=f"win_{i}"):
                                    if salvar_nuvem(cid_selecionado, datetime.now(), "Venda Fechada", f"Ref {r['Resumo']}", u_log, r['Valor_Proposta']): st.rerun()
                                if cc.button("❌", key=f"loss_{i}"):
                                    if salvar_nuvem(cid_selecionado, datetime.now(), "Venda Perdida", f"Ref {r['Resumo']}", u_log, r['Valor_Proposta']): st.rerun()
                    else:
                        st.info("Nenhuma proposta pendente.")

                with tab3:
                    with st.form(key="nova_acao_form"):
                        act = st.selectbox("Ação", ["Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Agendou Visita"])
                        vlr = st.number_input("Valor (R$)", step=1) if act == "Orçamento Enviado" else 0
                        obs = st.text_area("Observações")
                        submit = st.form_submit_button("💾 Salvar Interação")
                        
                        if submit:
                            if salvar_nuvem(cid_selecionado, datetime.now(), act, obs, u_log, vlr):
                                st.success("Salvo com sucesso!")
                                time.sleep(1)
                                st.rerun()
        else:
            st.info("👈 Selecione um cliente na lista ao lado para ver os detalhes.")
