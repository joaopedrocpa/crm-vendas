import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="CRM Master 7.7", layout="wide")

# --- MENSAGEM DE CARREGAMENTO (Placeholder) ---
placeholder = st.empty()
placeholder.info("⏳ Carregando sistema... Conectando ao Google Sheets.")

# --- FUNÇÕES VISUAIS E FORMATAÇÃO ---
def formatar_moeda_visual(valor):
    if pd.isna(valor) or str(valor).strip() == '': return "R$ 0,00"
    try:
        return f"R$ {float(valor):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    except: return str(valor)

def limpar_valor_monetario(valor):
    if pd.isna(valor): return 0.0
    s = str(valor).strip().replace('R$', '').strip()
    try:
        return float(s)
    except:
        if '.' in s and ',' in s: s = s.replace('.', '').replace(',', '.')
        elif ',' in s: s = s.replace(',', '.')
        try: return float(s)
        except: return 0.0

def formatar_documento(valor):
    """Aplica máscara de CPF (até 11 dígitos) ou CNPJ (>11 dígitos)"""
    if pd.isna(valor) or str(valor).strip() == '': return "-"
    doc = ''.join(filter(str.isdigit, str(valor)))
    if not doc: return "-"
    if len(doc) > 11:
        doc = doc.zfill(14)
        return f"{doc[:2]}.{doc[2:5]}.{doc[5:8]}/{doc[8:12]}-{doc[12:]}"
    else:
        doc = doc.zfill(11)
        return f"{doc[:3]}.{doc[3:6]}.{doc[6:9]}-{doc[9:]}"

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
        st.error(f"⚠️ Erro de Conexão com Google Sheets: {e}")
        return None

# --- CARREGAMENTO ---
@st.cache_data(ttl=60)
def carregar_dados_completos():
    spreadsheet = conectar_google_sheets()
    if spreadsheet is None: return None, None, None
    try:
        try:
            sheet_clientes = spreadsheet.worksheet("Clientes")
            df_protheus = pd.DataFrame(sheet_clientes.get_all_records())
        except Exception as e:
            st.error(f"Erro ao ler aba 'Clientes': {e}")
            return None, None, None
        
        try:
            sheet_leads = spreadsheet.worksheet("Novos_Leads")
            dados_leads = sheet_leads.get_all_records()
            df_leads = pd.DataFrame(dados_leads)
        except: df_leads = pd.DataFrame() 
            
        if not df_leads.empty:
            df_leads = df_leads.astype(str)
            df_protheus = df_protheus.astype(str)
            df_clientes = pd.concat([df_protheus, df_leads], ignore_index=True)
        else:
            df_clientes = df_protheus

        if not df_clientes.empty:
            df_clientes.columns = df_clientes.columns.str.strip()
            df_clientes['ID_Cliente_CNPJ_CPF'] = df_clientes['ID_Cliente_CNPJ_CPF'].astype(str)
            if 'Total_Compras' in df_clientes.columns:
                df_clientes['Total_Compras'] = df_clientes['Total_Compras'].apply(limpar_valor_monetario)
            if 'Data_Ultima_Compra' in df_clientes.columns:
                df_clientes['Data_Ultima_Compra'] = pd.to_datetime(df_clientes['Data_Ultima_Compra'], dayfirst=True, errors='coerce')

        try:
            sheet_interacoes = spreadsheet.worksheet("Interacoes")
            df_interacoes = pd.DataFrame(sheet_interacoes.get_all_records())
            if not df_interacoes.empty:
                if 'Valor_Proposta' in df_interacoes.columns:
                    df_interacoes['Valor_Proposta'] = df_interacoes['Valor_Proposta'].apply(limpar_valor_monetario)
                else: df_interacoes['Valor_Proposta'] = 0.0
                
                if 'Data' in df_interacoes.columns:
                    df_interacoes['Data_Obj'] = pd.to_datetime(df_interacoes['Data'], dayfirst=True, errors='coerce').dt.date
                
                if 'CNPJ_Cliente' in df_interacoes.columns:
                    mapa_nomes = dict(zip(df_clientes['ID_Cliente_CNPJ_CPF'].astype(str), df_clientes['Nome_Fantasia']))
                    df_interacoes['CNPJ_Cliente'] = df_interacoes['CNPJ_Cliente'].astype(str)
                    df_interacoes['Nome_Cliente'] = df_interacoes['CNPJ_Cliente'].map(mapa_nomes).fillna("Cliente Carteira")
        except:
            df_interacoes = pd.DataFrame(columns=['CNPJ_Cliente', 'Data', 'Tipo', 'Resumo', 'Vendedor', 'Valor_Proposta'])

        try:
            sheet_config = spreadsheet.worksheet("Config_Equipe")
            df_config = pd.DataFrame(sheet_config.get_all_records())
        except:
            df_config = pd.DataFrame(columns=['Usuario_Login', 'Carteiras_Visiveis'])

        return df_clientes, df_interacoes, df_config
    except Exception as e:
        st.error(f"Erro ao processar dados: {e}")
        return None, None, None

# --- SALVAMENTO ---
def salvar_interacao_nuvem(cnpj, data_obj, tipo, resumo, vendedor, valor=0.0):
    try:
        spreadsheet = conectar_google_sheets()
        sheet = spreadsheet.worksheet("Interacoes")
        data_str = data_obj.strftime('%d/%m/%Y')
        valor_str = f"{valor:.2f}".replace('.', ',') 
        sheet.append_row([str(cnpj), data_str, tipo, resumo, vendedor, valor_str])
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro Salvar: {e}")
        return False

def salvar_novo_lead_completo(cnpj, nome, contato, telefone, vendedor, origem, primeira_acao, resumo_inicial, valor_inicial=0.0):
    try:
        spreadsheet = conectar_google_sheets()
        sheet_leads = spreadsheet.worksheet("Novos_Leads")
        nova_linha = [str(cnpj), nome.upper(), contato, "NOVO LEAD", telefone, "", "", "0", "", "0", "", vendedor, origem]
        sheet_leads.append_row(nova_linha)
        
        sheet_interacoes = spreadsheet.worksheet("Interacoes")
        data_str = datetime.now().strftime('%d/%m/%Y')
        valor_str = f"{valor_inicial:.2f}".replace('.', ',')
        sheet_interacoes.append_row([str(cnpj), data_str, primeira_acao, resumo_inicial, vendedor, valor_str])
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro Lead: {e}")
        return False

# --- CALLBACKS ---
def processar_salvamento_lead(usuario_logado):
    nome = st.session_state["novo_nome"]
    doc = st.session_state["novo_doc"]
    origem = st.session_state["novo_origem"]
    acao = st.session_state["novo_acao"]
    contato = st.session_state["novo_contato"]
    tel = st.session_state["novo_tel"]
    resumo = st.session_state["novo_resumo"]
    val = st.session_state["novo_val"]

    if not nome or not doc or origem == "SELECIONE..." or acao == "SELECIONE...":
        st.error("Preencha campos obrigatórios!")
    else:
        sucesso = salvar_novo_lead_completo(doc, nome, contato, tel, usuario_logado, origem, acao, resumo, val)
        if sucesso:
            st.success("Lead Salvo!")
            for k in ["novo_nome", "novo_doc", "novo_contato", "novo_tel", "novo_resumo"]:
                st.session_state[k] = ""
            st.session_state["novo_val"] = 0.0
            st.session_state["novo_origem"] = "SELECIONE..."
            st.session_state["novo_acao"] = "SELECIONE..."

def processar_salvamento_vendedor(cid, usuario_logado, tipo_selecionado):
    obs = st.session_state["obs_temp"]
    val = st.session_state["val_temp"]
    sucesso = salvar_interacao_nuvem(cid, datetime.now(), tipo_selecionado, obs, usuario_logado, val)
    if sucesso:
        st.success("Salvo!")
        st.session_state["obs_temp"] = ""
        st.session_state["val_temp"] = 0.0

# --- APP ---
try:
    # 1. MOSTRA MENU LATERAL IMEDIATAMENTE (Para não ficar tela preta total)
    st.sidebar.title("🚀 CRM Master 7.7")
    
    # 2. CARREGA DADOS
    df, df_interacoes, df_config = carregar_dados_completos()
    
    # 3. LIMPA A MENSAGEM DE CARREGAMENTO
    placeholder.empty()

    if df is not None and not df.empty:
        # --- LÓGICA DO SISTEMA ---
        hoje = datetime.now().date()
        
        if 'Data_Ultima_Compra' in df.columns:
            df['Dias_Sem_Comprar'] = (pd.Timestamp(hoje) - df['Data_Ultima_Compra']).dt.days
        else: df['Dias_Sem_Comprar'] = 0

        def calcular_status(linha):
            cnpj = linha['ID_Cliente_CNPJ_CPF']
            if not df_interacoes.empty and 'CNPJ_Cliente' in df_interacoes.columns:
                cnpj_str = str(cnpj)
                filtro = df_interacoes[df_interacoes['CNPJ_Cliente'] == cnpj_str]
                if not filtro.empty and 'Data_Obj' in filtro.columns:
                    filtro = filtro.sort_values(by='Data_Obj')
                    ultima = filtro.iloc[-1]
                    try:
                        if pd.notna(ultima['Data_Obj']):
                            dias_acao = (hoje - ultima['Data_Obj']).days
                            if ultima['Tipo'] == 'Orçamento Enviado': return '⚠️ FOLLOW-UP' if dias_acao >= 5 else '⏳ NEGOCIAÇÃO'
                            if ultima['Tipo'] == 'Venda Fechada': return '⭐ VENDA RECENTE'
                            if ultima['Tipo'] == 'Venda Perdida': return '👎 VENDA PERDIDA'
                            if ultima['Tipo'] == 'Ligação Realizada': return '📞 CONTATADO RECENTEMENTE'
                            if ultima['Tipo'] == 'WhatsApp Enviado': return '💬 WHATSAPP INICIADO'
                    except: pass
            if pd.isna(linha['Dias_Sem_Comprar']): return '🆕 NOVO S/ INTERAÇÃO'
            if linha['Dias_Sem_Comprar'] >= 60: return '🔴 RECUPERAR'
            return '🟢 ATIVO'

        df['Status'] = df.apply(calcular_status, axis=1)

        # Login
        if df_config.empty:
            if 'Ultimo_Vendedor' in df.columns: usuarios_disponiveis = df['Ultimo_Vendedor'].dropna().unique().tolist()
            else: usuarios_disponiveis = []
            usuarios_disponiveis.insert(0, "GESTOR")
        else: usuarios_disponiveis = df_config['Usuario_Login'].unique().tolist()
        usuario_logado = st.sidebar.selectbox("Usuário:", usuarios_disponiveis)

        # --- CADASTRO ---
        if usuario_logado != "GESTOR":
            st.sidebar.markdown("---")
            with st.sidebar.expander("➕ Cadastrar Novo Lead"):
                if "novo_nome" not in st.session_state: st.session_state["novo_nome"] = ""
                if "novo_doc" not in st.session_state: st.session_state["novo_doc"] = ""
                if "novo_contato" not in st.session_state: st.session_state["novo_contato"] = ""
                if "novo_tel" not in st.session_state: st.session_state["novo_tel"] = ""
                if "novo_resumo" not in st.session_state: st.session_state["novo_resumo"] = ""
                if "novo_val" not in st.session_state: st.session_state["novo_val"] = 0.0
                if "novo_origem" not in st.session_state: st.session_state["novo_origem"] = "SELECIONE..."
                if "novo_acao" not in st.session_state: st.session_state["novo_acao"] = "SELECIONE..."

                st.text_input("Nome:", key="novo_nome")
                st.text_input("CPF/CNPJ:", key="novo_doc")
                st.text_input("Contato:", key="novo_contato")
                st.text_input("Telefone:", key="novo_tel")
                
                c1, c2 = st.columns(2)
                c1.selectbox("Origem:", ["SELECIONE...", "SZ.CHAT", "LIGAÇÃO", "PRESENCIAL", "E-MAIL", "INDICAÇÃO"], key="novo_origem")
                c2.selectbox("Ação:", ["SELECIONE...", "Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Agendou Visita"], key="novo_acao")
                
                if st.session_state["novo_acao"] == "Orçamento Enviado":
                    st.number_input("Valor (R$):", step=0.01, format="%.2f", key="novo_val")
                st.text_area("Resumo:", key="novo_resumo")
                st.button("💾 SALVAR LEAD", type="primary", on_click=processar_salvamento_lead, args=(usuario_logado,))

        # --- GESTOR ---
        if usuario_logado == "GESTOR":
            st.title("📊 Painel Financeiro")
            if 'Ultimo_Vendedor' in df.columns: meus_clientes = df
            else: meus_clientes = pd.DataFrame()

            with st.container(border=True):
                col_f1, col_f2, col_f3 = st.columns(3)
                d_ini = col_f1.date_input("De:", value=hoje - timedelta(days=30), format="DD/MM/YYYY")
                d_fim = col_f2.date_input("Até:", value=hoje, format="DD/MM/YYYY")
                opcoes_tipo = df_interacoes['Tipo'].unique().tolist() if not df_interacoes.empty else []
                tipos_sel = col_f3.multiselect("Filtrar Tipos:", options=opcoes_tipo, default=opcoes_tipo)

            if not df_interacoes.empty and 'Data_Obj' in df_interacoes.columns:
                mask_data = (df_interacoes['Data_Obj'] >= d_ini) & (df_interacoes['Data_Obj'] <= d_fim)
                df_filtered = df_interacoes[mask_data].copy()
                
                vlr_orcado = df_filtered[df_filtered['Tipo'] == 'Orçamento Enviado']['Valor_Proposta'].sum()
                vlr_perdido = df_filtered[df_filtered['Tipo'] == 'Venda Perdida']['Valor_Proposta'].sum()
                vlr_fechado = df_filtered[df_filtered['Tipo'] == 'Venda Fechada']['Valor_Proposta'].sum()
                qtd_fechado = len(df_filtered[df_filtered['Tipo'] == 'Venda Fechada'])
                qtd_total = len(df_filtered)
                df_tabela = df_filtered[df_filtered['Tipo'].isin(tipos_sel)]
            else:
                vlr_orcado = vlr_perdido = vlr_fechado = 0.0
                qtd_fechado = qtd_total = 0
                df_filtered = pd.DataFrame()
                df_tabela = pd.DataFrame()

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("💰 Orçado", formatar_moeda_visual(vlr_orcado))
            k2.metric("👎 Perdido", formatar_moeda_visual(vlr_perdido))
            k3.metric("✅ Fechado", formatar_moeda_visual(vlr_fechado), f"{qtd_fechado} vendas")
            k4.metric("📞 Interações", f"{qtd_total}")
            
            st.divider()

            t1, t2 = st.tabs(["🏆 Ranking", "📝 Detalhes"])
            with t1:
                if not df_filtered.empty:
                    df_filtered['Is_Orcamento'] = (df_filtered['Tipo'] == 'Orçamento Enviado').astype(int)
                    df_filtered['Is_Fechado'] = (df_filtered['Tipo'] == 'Venda Fechada').astype(int)
                    df_filtered['Valor_Aux_Ranking'] = df_filtered.apply(lambda x: x['Valor_Proposta'] if x['Tipo'] == 'Venda Fechada' else 0.0, axis=1)

                    ranking = df_filtered.groupby('Vendedor').agg(
                        Orcamentos=('Is_Orcamento', 'sum'),
                        Fechados=('Is_Fechado', 'sum'),
                        Total_Vendido=('Valor_Aux_Ranking', 'sum')
                    ).reset_index().sort_values('Total_Vendido', ascending=False)
                    ranking['Total_Vendido'] = ranking['Total_Vendido'].apply(formatar_moeda_visual)
                    st.dataframe(ranking, use_container_width=True)
                else: st.info("Sem dados.")
            with t2:
                if not df_tabela.empty:
                    view = df_tabela[['Data_Obj', 'Nome_Cliente', 'Tipo', 'Resumo', 'Valor_Proposta', 'Vendedor']].copy()
                    view['Valor_Proposta'] = view['Valor_Proposta'].apply(formatar_moeda_visual)
                    view.rename(columns={'Data_Obj': 'Data'}, inplace=True)
                    st.dataframe(
                        view, 
                        use_container_width=True, 
                        hide_index=True,
                        column_config={
                            "Data": st.column_config.DateColumn("Data", format="DD/MM/YYYY")
                        }
                    )
                else: st.info("Nenhuma interação.")

        # --- VENDEDOR ---
        else:
            st.title(f"Área: {usuario_logado}")
            
            # 1. FILTRAGEM DE CARTEIRA
            if not df_config.empty:
                regra = df_config[df_config['Usuario_Login'] == usuario_logado]
                if not regra.empty:
                    carteiras = regra.iloc[0]['Carteiras_Visiveis']
                    if "TODOS" in carteiras.upper(): meus_clientes = df 
                    else:
                        lista = [n.strip() for n in carteiras.split(',')]
                        if 'Ultimo_Vendedor' in df.columns: meus_clientes = df[df['Ultimo_Vendedor'].isin(lista)]
                        else: meus_clientes = pd.DataFrame()
                else: meus_clientes = pd.DataFrame()
            else:
                if 'Ultimo_Vendedor' in df.columns: meus_clientes = df[df['Ultimo_Vendedor'] == usuario_logado]
                else: meus_clientes = pd.DataFrame()

            # INICIALIZA CID COM NONE PARA NÃO QUEBRAR
            cid = None

            if meus_clientes.empty:
                st.error("Sua carteira está vazia ou não foi carregada corretamente.")
            else:
                c_esq, c_dir = st.columns([1, 1])
                with c_esq:
                    st.subheader("Carteira")
                    termo_busca = st.text_input("🔍 Buscar por CNPJ/CPF ou Nome:", placeholder="Digite para buscar...")
                    
                    if termo_busca:
                        termo_busca = termo_busca.upper()
                        lista = meus_clientes[
                            meus_clientes['Nome_Fantasia'].str.upper().str.contains(termo_busca, na=False) |
                            meus_clientes['ID_Cliente_CNPJ_CPF'].astype(str).str.contains(termo_busca, na=False)
                        ]
                        if lista.empty: st.warning("Nenhum cliente encontrado com esse termo.")
                    else:
                        ops = ['🔴 RECUPERAR', '⚠️ FOLLOW-UP', '⏳ NEGOCIAÇÃO', '💬 WHATSAPP INICIADO', '👎 VENDA PERDIDA', '⭐ VENDA RECENTE', '🟢 ATIVO']
                        sel_status = st.multiselect("Status:", ops, default=['🔴 RECUPERAR', '⚠️ FOLLOW-UP', '⏳ NEGOCIAÇÃO'])
                        lista = meus_clientes[meus_clientes['Status'].isin(sel_status)].sort_values('Status', ascending=False)
                        if lista.empty: st.info("Nenhum cliente nestes status.")

                    if not lista.empty:
                        if len(lista) > 500 and not termo_busca:
                            st.warning(f"Mostrando os primeiros 500 de {len(lista)} clientes. Use a busca para refinar.")
                            lista = lista.head(500)
                        
                        # AQUI CID RECEBE VALOR
                        cid = st.radio("Cliente:", lista['ID_Cliente_CNPJ_CPF'].tolist(), format_func=lambda x: f"{lista[lista['ID_Cliente_CNPJ_CPF']==x]['Nome_Fantasia'].values[0]}")

                with c_dir:
                    if cid and not meus_clientes[meus_clientes['ID_Cliente_CNPJ_CPF'] == cid].empty:
                        cli = meus_clientes[meus_clientes['ID_Cliente_CNPJ_CPF'] == cid].iloc[0]
                        with st.container(border=True):
                            st.markdown(f"### {cli['Nome_Fantasia']}")
                            doc_fmt = formatar_documento(cli['ID_Cliente_CNPJ_CPF'])
                            st.caption(f"CNPJ/CPF: {doc_fmt}")
                            dono = cli['Ultimo_Vendedor'] if 'Ultimo_Vendedor' in cli else "N/A"
                            st.markdown(f"**👤 Carteira:** {dono}")
                            st.info(f"Status: **{cli['Status']}**")
                            
                            c1, c2 = st.columns(2)
                            tel_val = cli['Telefone_Contato1'] if 'Telefone_Contato1' in cli else "-"
                            dt_compra = "-"
                            if 'Data_Ultima_Compra' in cli and pd.notna(cli['Data_Ultima_Compra']):
                                dt_compra = cli['Data_Ultima_Compra'].strftime('%d/%m/%Y')
                            c1.write(f"📞 **Tel:** {tel_val}")
                            c2.write(f"📅 **Compra:** {dt_compra}")

                            if not df_interacoes.empty and 'CNPJ_Cliente' in df_interacoes.columns:
                                hist = df_interacoes[df_interacoes['CNPJ_Cliente'] == str(cid)].sort_values('Data_Obj', ascending=False)
                                if not hist.empty:
                                    last = hist.iloc[0]
                                    dt_fmt = pd.to_datetime(last['Data']).strftime('%d/%m/%Y')
                                    st.warning(f"🕒 **Última Ação ({dt_fmt}):** {last['Tipo']}\n\n_{last['Resumo']}_")
                                    if last['Tipo'] == 'Orçamento Enviado' and last['Valor_Proposta'] > 0:
                                        v_fmt = formatar_moeda_visual(last['Valor_Proposta'])
                                        st.info(f"💰 **Proposta Aberta:** {v_fmt}")
                            st.divider()
                            
                            if "obs_temp" not in st.session_state: st.session_state["obs_temp"] = ""
                            if "val_temp" not in st.session_state: st.session_state["val_temp"] = 0.0
                            
                            tipo = st.selectbox("Ação:", ["Ligação Realizada", "WhatsApp Enviado", "Orçamento Enviado", "Venda Fechada", "Venda Perdida", "Agendou Visita"])
                            if tipo in ["Orçamento Enviado", "Venda Fechada", "Venda Perdida"]:
                                st.number_input("Valor (R$):", step=0.01, format="%.2f", key="val_temp")
                            st.text_area("Obs:", key="obs_temp")
                            st.button("✅ Salvar", type="primary", on_click=processar_salvamento_vendedor, args=(cid, usuario_logado, tipo))
                    else:
                        # MENSAGEM QUANDO NENHUM CLIENTE ESTÁ SELECIONADO
                        st.info("👈 Selecione um cliente na lista à esquerda para ver os detalhes.")
    else:
        # TELA DE ERRO QUANDO A PLANILHA NÃO CARREGA
        st.error("Não foi possível carregar os dados. Verifique a conexão com o Google Sheets.")
        if st.button("🔄 Tentar Novamente"):
            st.cache_data.clear()
            st.rerun()

except Exception as e:
    st.error(f"Erro Fatal no Sistema: {e}")
