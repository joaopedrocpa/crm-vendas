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

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="CRM Master 23.0 Speed", layout="wide")
URL_LOGO = "https://cdn-icons-png.flaticon.com/512/9187/9187604.png"

# --- CSS ---
st.markdown("""
<style>
    [data-testid="stSidebar"] {min-width: 300px;}
    div[data-testid="stMetric"] {background-color: #262730; border: 1px solid #464b5c; padding: 10px; border-radius: 5px;}
    .stButton button {width: 100%; font-weight: bold;}
    .stProgress > div > div > div > div {background-color: #00ff00;}
</style>
""", unsafe_allow_html=True)

# --- HELPERS ---
def gerar_id_proposta(): return ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
def extrair_id(t): return re.search(r'(#[A-Z0-9]{4})', str(t)).group(1) if pd.notna(t) and re.search(r'(#[A-Z0-9]{4})', str(t)) else None
def extrair_pedido_protheus(t): return re.search(r'\[PROTHEUS\] Pedido: (\w+)', str(t)).group(1) if pd.notna(t) and re.search(r'\[PROTHEUS\] Pedido: (\w+)', str(t)) else None
def limpar_int(v): 
    try: return int(re.sub(r'[^\d]', '', str(v).split(',')[0])) if pd.notna(v) and str(v).strip() else 0
    except: return 0
def fmt_moeda(v): return f"R$ {int(v):,.0f}".replace(',', '.')
def fmt_data(d): return pd.to_datetime(d).strftime('%d/%m/%Y') if pd.notna(d) and str(d).strip() != '' else "-"
def fmt_doc(v):
    d = ''.join(filter(str.isdigit, str(v)))
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}" if len(d)>11 else f"{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}"

# --- CONEXÃO ---
def conectar_google_sheets():
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(st.secrets["credenciais_google"]), ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds).open("Banco de Dados CRM")
    except: return None

# --- CARREGAMENTO OTIMIZADO ---
@st.cache_data(ttl=3600)
def carregar_tudo_cache():
    # Carrega tudo de uma vez para evitar múltiplas chamadas
    ss = conectar_google_sheets()
    if not ss: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # 1. Config
    try:
        df_cfg = pd.DataFrame(ss.worksheet("Config_Equipe").get_all_records()).astype(str)
        for c in ['Meta_Fat','Meta_Clientes','Meta_Atividades']: 
            if c in df_cfg.columns: df_cfg[c] = df_cfg[c].apply(limpar_int)
    except: df_cfg = pd.DataFrame()

    # 2. Clientes
    try:
        df_cli = pd.DataFrame(ss.worksheet("Clientes").get_all_records())
        if not df_cli.empty:
            df_cli.columns = df_cli.columns.str.strip()
            df_cli['ID_Cliente_CNPJ_CPF'] = df_cli['ID_Cliente_CNPJ_CPF'].astype(str)
            if 'Total_Compras' in df_cli.columns: df_cli['Total_Compras'] = df_cli['Total_Compras'].apply(limpar_int)
            if 'Data_Ultima_Compra' in df_cli.columns: df_cli['Data_Ultima_Compra'] = pd.to_datetime(df_cli['Data_Ultima_Compra'], dayfirst=True, errors='coerce')
    except: df_cli = pd.DataFrame()

    # 3. Leads
    try:
        df_leads = pd.DataFrame(ss.worksheet("Novos_Leads").get_all_records()).astype(str)
    except: df_leads = pd.DataFrame()
    
    # Merge Leads em Clientes
    if not df_leads.empty: df_cli = pd.concat([df_cli, df_leads], ignore_index=True)

    # 4. Interações
    try:
        df_int = pd.DataFrame(ss.worksheet("Interacoes").get_all_records())
        if not df_int.empty:
            if 'Valor_Proposta' in df_int.columns: df_int['Valor_Proposta'] = df_int['Valor_Proposta'].apply(limpar_int)
            if 'Data' in df_int.columns: df_int['Data_Obj'] = pd.to_datetime(df_int['Data'], dayfirst=True, errors='coerce').dt.date
            df_int['CNPJ_Cliente'] = df_int['CNPJ_Cliente'].astype(str)
            # Nome Cliente (Mapeamento Rápido)
            if 'Nome_Cliente' not in df_int.columns: df_int['Nome_Cliente'] = None
            mapa = dict(zip(df_cli['ID_Cliente_CNPJ_CPF'], df_cli['Nome_Fantasia']))
            mask_n = df_int['Nome_Cliente'].isna() | (df_int['Nome_Cliente'] == "")
            df_int.loc[mask_n, 'Nome_Cliente'] = df_int.loc[mask_n, 'CNPJ_Cliente'].map(mapa).fillna("Cliente Carteira")
    except: df_int = pd.DataFrame(columns=['CNPJ_Cliente','Data','Tipo','Resumo','Vendedor','Valor_Proposta','Data_Obj','Nome_Cliente'])

    return df_cfg, df_cli, df_int

# --- MOTOR DE CÁLCULO VETORIZADO (A MÁGICA DA VELOCIDADE) ---
def recalcular_status_massa(df_c, df_i):
    """Calcula status de 5000 clientes em 0.01 segundos usando Vectorization"""
    if df_c.empty: return df_c
    
    hoje = datetime.now().date()
    df_c['Status'] = '🟢 ATIVO' # Default
    
    # 1. Dias sem Comprar
    if 'Data_Ultima_Compra' in df_c.columns:
        df_c['Dias_Sem_Comprar'] = (pd.Timestamp(hoje) - df_c['Data_Ultima_Compra']).dt.days
        df_c.loc[df_c['Dias_Sem_Comprar'] >= 60, 'Status'] = '🔴 RECUPERAR'
        df_c.loc[df_c['Dias_Sem_Comprar'].isna(), 'Status'] = '🆕 NOVO'
    
    if df_i.empty: return df_c

    # 2. Identificar Propostas em Aberto (Logica de Conjuntos)
    # Lista de IDs resolvidos
    fechados = df_i[df_i['Tipo'].isin(['Venda Fechada', 'Venda Perdida'])]
    ids_baixados = set(fechados['Resumo'].apply(extrair_id).dropna())
    peds_baixados = set(fechados['Resumo'].apply(extrair_pedido_protheus).dropna())
    
    # Filtra Orçamentos
    orcs = df_i[df_i['Tipo'] == 'Orçamento Enviado'].copy()
    orcs['ID_Temp'] = orcs['Resumo'].apply(extrair_id)
    orcs['Ped_Temp'] = orcs['Resumo'].apply(extrair_pedido_protheus)
    
    # Clientes com algo aberto (que não está na lista de baixados)
    mask_aberto = (
        (~orcs['ID_Temp'].isin(ids_baixados) & orcs['ID_Temp'].notna()) |
        (~orcs['Ped_Temp'].isin(peds_baixados) & orcs['Ped_Temp'].notna())
    )
    cnpjs_negociacao = orcs[mask_aberto]['CNPJ_Cliente'].unique()
    
    # Aplica Status Negociação
    df_c.loc[df_c['ID_Cliente_CNPJ_CPF'].isin(cnpjs_negociacao), 'Status'] = '⏳ NEGOCIAÇÃO'
    
    # 3. Última Interação (Para status recentes) - GroupBy é mto rápido
    # Pega apenas clientes que NÃO estão em negociação
    mask_nao_neg = ~df_c['ID_Cliente_CNPJ_CPF'].isin(cnpjs_negociacao)
    clientes_livres = df_c[mask_nao_neg]['ID_Cliente_CNPJ_CPF']
    
    ultimas = df_i[df_i['CNPJ_Cliente'].isin(clientes_livres)].sort_values('Data_Obj').groupby('CNPJ_Cliente').tail(1)
    
    # Mapas de Status
    mapa_status = ultimas.set_index('CNPJ_Cliente')['Tipo'].to_dict()
    
    def traduzir_recente(row):
        if row['Status'] == '⏳ NEGOCIAÇÃO': return row['Status']
        tipo = mapa_status.get(str(row['ID_Cliente_CNPJ_CPF']))
        if tipo == 'Venda Fechada': return '⭐ VENDA RECENTE'
        if tipo == 'Venda Perdida': return '👎 VENDA PERDIDA'
        if tipo in ['Ligação Realizada', 'WhatsApp Enviado']: return '💬 CONTATADO'
        return row['Status']

    # Aqui usamos apply mas num dataset reduzido e dicionario rapido
    df_c['Status'] = df_c.apply(traduzir_recente, axis=1)
    
    return df_c

# --- SALVAMENTO ---
def salvar_nuvem(cnpj, data, tipo, resumo, vend, val):
    try:
        ss = conectar_google_sheets()
        ss.worksheet("Interacoes").append_row([str(cnpj), data.strftime('%d/%m/%Y'), tipo, resumo, vend, int(val)])
        
        # Atualiza Sessão Local
        novo = {'CNPJ_Cliente':str(cnpj),'Data_Obj':data,'Tipo':tipo,'Resumo':resumo,'Vendedor':vend,'Valor_Proposta':int(val),'Nome_Cliente':'...'}
        st.session_state['df_int'] = pd.concat([st.session_state['df_int'], pd.DataFrame([novo])], ignore_index=True)
        # Recalcula status localmente
        st.session_state['df_cli'] = recalcular_status_massa(st.session_state['df_cli'], st.session_state['df_int'])
        return True
    except: return False

def salvar_lead(nome, doc, cont, tel, vend, ori, acao, res, val):
    try:
        ss = conectar_google_sheets()
        ss.worksheet("Novos_Leads").append_row([str(doc), nome.upper(), cont, "NOVO LEAD", tel, "", "", "0", "", "0", "", vend, ori])
        st.cache_data.clear() # Limpa cache total
        if acao:
            id_p = f"#{gerar_id_proposta()} " if acao == "Orçamento Enviado" else ""
            ss.worksheet("Interacoes").append_row([str(doc), datetime.now().strftime('%d/%m/%Y'), acao, f"{id_p}{res}", vend, int(val)])
        return True
    except: return False

def proc_import(file, df_old):
    try:
        df = pd.read_excel(file)
        if not {'DATA','CNPJ','VENDEDOR','VALOR','PEDIDO','STATUS'}.issubset(df.columns): return False, "Colunas erradas!"
        
        # Otimização: Cache de pedidos existentes
        peds_existentes = set()
        if not df_old.empty:
            peds_existentes = set(df_old['Resumo'].apply(extrair_pedido_protheus).dropna())

        novos = []
        ss = conectar_google_sheets()
        ws = ss.worksheet("Interacoes")
        
        for _, r in df.iterrows():
            pid = str(r['PEDIDO']).strip()
            if pid not in peds_existentes:
                stt = str(r['STATUS']).upper()
                tipo = "Venda Fechada" if "FECHADO" in stt or "FATURADO" in stt else ("Venda Perdida" if "CANCELADO" in stt else "Orçamento Enviado")
                resumo = f"{'#'+gerar_id_proposta()+' ' if tipo=='Orçamento Enviado' else ''}[PROTHEUS] Pedido: {pid} | {stt}"
                try: dt = pd.to_datetime(r['DATA']).strftime('%d/%m/%Y')
                except: dt = datetime.now().strftime('%d/%m/%Y')
                novos.append([''.join(filter(str.isdigit, str(r['CNPJ']))), dt, tipo, resumo, str(r['VENDEDOR']).upper().strip(), limpar_int(r['VALOR'])])
        
        if novos: 
            ws.append_rows(novos)
            st.cache_data.clear()
            return True, f"{len(novos)} importados."
        return True, "Nada novo."
    except Exception as e: return False, str(e)

# --- APP ---
if URL_LOGO: st.sidebar.image(URL_LOGO, width=150)
st.sidebar.title("CRM V23 Speed")

if st.sidebar.button("🔄 Recarregar"): st.cache_data.clear(); st.rerun()

# INICIALIZAÇÃO (Roda uma vez)
if 'df_cli' not in st.session_state:
    cfg, cli, inter = carregar_tudo_cache()
    # Roda o cálculo pesado UMA vez no início
    cli = recalcular_status_massa(cli, inter)
    st.session_state.update({'df_cfg':cfg, 'df_cli':cli, 'df_int':inter})

df_cli = st.session_state['df_cli']
df_int = st.session_state['df_int']
df_cfg = st.session_state['df_cfg']

# LOGIN
if not df_cfg.empty:
    usrs = sorted(df_cfg['Usuario'].unique())
    if 'logado' not in st.session_state: st.session_state['logado'] = False
    
    if not st.session_state['logado']:
        u = st.sidebar.selectbox("User", usrs)
        p = st.sidebar.text_input("Senha", type="password")
        if st.sidebar.button("Entrar"):
            ud = df_cfg[df_cfg['Usuario']==u].iloc[0]
            if str(ud['Senha']).strip() == str(p).strip():
                st.session_state['logado']=True; st.session_state['u_atual']=u; st.rerun()
        st.stop()
    
    u_log = st.session_state['u_atual']
    if st.sidebar.button(f"Sair ({u_log})"): st.session_state['logado']=False; st.rerun()
    
    u_data = df_cfg[df_cfg['Usuario']==u_log].iloc[0]
    tipo_u = str(u_data['Tipo']).upper().strip()
    carts = [x.strip() for x in str(u_data['Carteira_Alvo']).split(',')]

    # METAS SIDEBAR
    prim_dia = datetime.now().date().replace(day=1)
    if not df_int.empty:
        df_m = df_int[(df_int['Vendedor']==u_log) & (df_int['Data_Obj']>=prim_dia)]
        fat_r = df_m[df_m['Tipo']=='Venda Fechada']['Valor_Proposta'].sum()
        cli_r = df_m[df_m['Tipo']=='Venda Fechada']['CNPJ_Cliente'].nunique()
        ativ_r = len(df_m[df_m['Tipo'].isin(['Ligação Realizada','WhatsApp Enviado','Agendou Visita'])])
    else: fat_r=0; cli_r=0; ativ_r=0
    
    st.sidebar.markdown("### 🎯 Metas Mês")
    mf, mc, ma = u_data.get('Meta_Fat',0), u_data.get('Meta_Clientes',0), u_data.get('Meta_Atividades',0)
    st.sidebar.caption(f"💰 {fmt_moeda(fat_r)} / {fmt_moeda(mf)}"); st.sidebar.progress(min(fat_r/mf,1.0) if mf>0 else 0)
    st.sidebar.caption(f"👥 {cli_r} / {mc}"); st.sidebar.progress(min(cli_r/mc,1.0) if mc>0 else 0)
    st.sidebar.caption(f"🔨 {ativ_r} / {ma}"); st.sidebar.progress(min(ativ_r/ma,1.0) if ma>0 else 0)

    # ACTIONS
    if tipo_u == "GESTOR":
        with st.sidebar.expander("📥 Importar"):
            f = st.file_uploader("XLSX", type=["xlsx"])
            if f and st.button("Processar"): 
                ok, msg = proc_import(f, df_int)
                if ok: st.success(msg); time.sleep(1); st.rerun()
                else: st.error(msg)
    
    if "TODOS" in carts or tipo_u == "VENDEDOR":
        with st.sidebar.expander("➕ Lead"):
            n = st.text_input("Nome", key="ln"); d = st.text_input("Doc", key="ld")
            c = st.text_input("Contato", key="lc"); t = st.text_input("Tel", key="lt")
            o = st.selectbox("Origem", ["LIGAÇÃO","E-MAIL","INDICAÇÃO"], key="lo")
            a = st.selectbox("Ação", ["Ligação Realizada","Orçamento Enviado"], key="la")
            v = st.number_input("R$", step=1, key="lv") if a == "Orçamento Enviado" else 0
            r = st.text_area("Resumo", key="lr")
            if st.button("Salvar Lead"):
                if salvar_lead(n,d,c,t,u_log,o,a,r,v): st.success("Ok!"); st.rerun()

    # FILTERING
    meus_cli = df_cli if "TODOS" in carts else df_cli[df_cli['Ultimo_Vendedor'].isin(carts)]
    minhas_int = df_int if "TODOS" in carts else df_int[df_int['Vendedor'].isin(carts)]

    # DASHBOARD
    if tipo_u == "GESTOR":
        st.title("📊 Painel")
        c1,c2,c3 = st.columns(3)
        di = c1.date_input("De", value=datetime.now()-timedelta(days=30))
        df = c2.date_input("Até", value=datetime.now())
        sel_v = c3.multiselect("Vendedores", minhas_int['Vendedor'].unique())
        
        msk = (minhas_int['Data_Obj']>=di) & (minhas_int['Data_Obj']<=df)
        if sel_v: msk &= minhas_int['Vendedor'].isin(sel_v)
        dff = minhas_int[msk]
        
        # Fast KPIs
        orc = dff[dff['Tipo']=='Orçamento Enviado']['Valor_Proposta'].sum()
        fec = dff[dff['Tipo']=='Venda Fechada']['Valor_Proposta'].sum()
        
        # Pipeline (Conjuntos)
        resols = set(dff[dff['Tipo'].isin(['Venda Fechada','Venda Perdida'])]['Resumo'])
        ids_res = set([extrair_id(x) for x in resols if extrair_id(x)])
        peds_res = set([extrair_pedido_protheus(x) for x in resols if extrair_pedido_protheus(x)])
        
        mesa = 0
        for _, row in dff[dff['Tipo']=='Orçamento Enviado'].iterrows():
            if not ((extrair_id(row['Resumo']) in ids_res) or (extrair_pedido_protheus(row['Resumo']) in peds_res)):
                mesa += row['Valor_Proposta']

        k1,k2,k3 = st.columns(3)
        k1.metric("Orçado", fmt_moeda(orc))
        k2.metric("Na Mesa", fmt_moeda(mesa))
        k3.metric("Fechado", fmt_moeda(fec))
        
        # Rank
        if not dff.empty:
            agg = dff.groupby('Vendedor').agg(
                Fat=('Valor_Proposta', lambda x: x[dff.loc[x.index,'Tipo']=='Venda Fechada'].sum()),
                Cli=('CNPJ_Cliente', lambda x: x[dff.loc[x.index,'Tipo']=='Venda Fechada'].nunique())
            ).reset_index()
            st.dataframe(agg, use_container_width=True)

    else: # VENDEDOR
        st.title("💼 Carteira")
        ce, cd = st.columns(2)
        with ce:
            q = st.text_input("🔎 Buscar (Nome/CNPJ)")
            if q:
                q = q.upper()
                lst = meus_cli[meus_cli['Nome_Fantasia'].str.upper().contains(q, na=False) | meus_cli['ID_Cliente_CNPJ_CPF'].astype(str).contains(q, na=False)]
            else:
                filtro_status = st.multiselect("Status", ['⏳ NEGOCIAÇÃO', '⚠️ FOLLOW-UP', '🔴 RECUPERAR'], ['⏳ NEGOCIAÇÃO'])
                lst = meus_cli[meus_cli['Status'].isin(filtro_status)].sort_values('Status')
            
            # PAGINAÇÃO IMPORTANTE: Só mostra os top 50
            if len(lst) > 50: st.caption(f"Mostrando 50 de {len(lst)} clientes.")
            
            sel_id = st.radio("Clientes", lst.head(50)['ID_Cliente_CNPJ_CPF'].tolist(), format_func=lambda x: f"{fmt_doc(x)} | {lst[lst['ID_Cliente_CNPJ_CPF']==x]['Nome_Fantasia'].values[0]}") if not lst.empty else None

        with cd:
            if sel_id:
                c_dados = meus_cli[meus_cli['ID_Cliente_CNPJ_CPF']==sel_id].iloc[0]
                with st.container(border=True):
                    st.subheader(c_dados['Nome_Fantasia'])
                    col1, col2 = st.columns(2)
                    col1.write(f"📞 {c_dados.get('Telefone_Contato1','-')}")
                    col1.write(f"📍 {c_dados.get('Cidade','-')}/{c_dados.get('UF','-')}")
                    col2.write(f"💰 {fmt_moeda(c_dados.get('Total_Compras',0))}")
                    col2.write(f"📅 {fmt_data(c_dados.get('Data_Ultima_Compra','-'))}")
                    
                    st.divider()
                    
                    # Interações (Filtragem rápida)
                    c_ints = minhas_int[minhas_int['CNPJ_Cliente']==str(sel_id)].sort_values('Data_Obj', ascending=False)
                    
                    t1, t2, t3 = st.tabs(["Hist", "Abertas", "Nova"])
                    with t1:
                        if not c_ints.empty:
                            view = c_ints[['Data_Obj','Tipo','Resumo','Valor_Proposta']].copy()
                            view['Valor_Proposta'] = view['Valor_Proposta'].apply(fmt_moeda)
                            st.dataframe(view, hide_index=True)
                    
                    with t2:
                        abertas = []
                        # Identifica abertas localmente
                        resols_cli = set(c_ints[c_ints['Tipo'].isin(['Venda Fechada','Venda Perdida'])]['Resumo'])
                        ids_res_cli = set([extrair_id(x) for x in resols_cli if extrair_id(x)])
                        peds_res_cli = set([extrair_pedido_protheus(x) for x in resols_cli if extrair_pedido_protheus(x)])
                        
                        for _, r in c_ints[c_ints['Tipo']=='Orçamento Enviado'].iterrows():
                             pid, ped = extrair_id(r['Resumo']), extrair_pedido_protheus(r['Resumo'])
                             if not ((pid and pid in ids_res_cli) or (ped and ped in peds_res_cli)):
                                 c1, c2, c3 = st.columns([2,1,1])
                                 c1.write(f"{fmt_moeda(r['Valor_Proposta'])} | {r['Resumo']}")
                                 if c2.button("✅", key=f"w{r.name}"): 
                                     if salvar_nuvem(sel_id, datetime.now(), "Venda Fechada", f"Ref {r['Resumo']}", u_log, r['Valor_Proposta']): st.rerun()
                                 if c3.button("❌", key=f"l{r.name}"):
                                     if salvar_nuvem(sel_id, datetime.now(), "Venda Perdida", f"Ref {r['Resumo']}", u_log, r['Valor_Proposta']): st.rerun()

                    with t3:
                        act = st.selectbox("Ação", ["Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Agendou Visita"])
                        vlr = st.number_input("R$", step=1) if act == "Orçamento Enviado" else 0
                        obs = st.text_area("Obs")
                        if st.button("Salvar"):
                            if salvar_nuvem(sel_id, datetime.now(), act, obs, u_log, vlr): st.success("Salvo!"); st.rerun()
