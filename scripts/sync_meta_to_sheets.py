"""
Meta広告 日次実績 -> Google スプレッドシート 同期スクリプト

GitHub Actions から実行される前提。認証情報はすべて環境変数（GitHub Secrets）経由で受け取り、
コード中に一切書き込まない。

必要な環境変数:
  META_ACCESS_TOKEN          Metaのアクセストークン（システムユーザートークン推奨）
  META_AD_ACCOUNT_ID         広告アカウントID（例: act_1234567890）
  GOOGLE_SERVICE_ACCOUNT_JSON  GCPサービスアカウントの認証鍵（JSON文字列そのもの）
  SPREADSHEET_ID             書き込み先スプレッドシートのID（URLの/d/と/editの間の文字列）

任意の環境変数:
  SHEET_NAME                 書き込み先シート名（省略時: "Meta広告データ"）
  BACKFILL_DAYS              指定した場合、シートの続きを無視してこの日数分を遡って取得する
                             （初回セットアップ時の一括取得や再取得に使用）
  API_VERSION                Graph APIバージョン（省略時: v23.0）
  ENABLE_AD_AGE_INSIGHTS     'true'にすると、広告単位×年齢層の月次実績（年齢別の上位/下位
                             クリエイティブ表示用）も取得する。広告数×年齢数だけデータ量が
                             増えるため、必要な案件のみリポジトリ変数で明示的に有効化する
                             （省略時は無効。CITIZEN Lではtrueを設定）
  INSTAGRAM_ACCOUNT_ID       InstagramビジネスアカウントID。設定した場合のみ、Instagramの
                             フォロワー数（日次の増減・推定総数）も取得する（省略時は無効）。
                             META_ACCESS_TOKENに instagram_basic・instagram_manage_insights
                             権限が必要
  INSTAGRAM_SHEET_NAME       Instagramフォロワー数の書き込み先シート名
                             （省略時: "Instagramフォロワー数"）
"""

import calendar
import json
import os
import time
from datetime import date, timedelta

import gspread
import requests
from google.oauth2.service_account import Credentials

API_VERSION = os.environ.get('API_VERSION', 'v23.0')
DEFAULT_BACKFILL_DAYS = 30

# Metaの一時的なエラー（過負荷・メンテナンス等）で、時間を置けば解消することが多いもの。
# code=1: 原因不明の一時エラー / code=2: サービス一時停止 / code=4,17: レート制限 / code=341: 一時的なブロック
RETRYABLE_ERROR_CODES = {1, 2, 4, 17, 341}
MAX_API_RETRIES = 3
RETRY_WAIT_SECONDS = 20


def graph_api_get(url, params, timeout=60):
    """
    Graph APIをGETし、一時的なエラー（RETRYABLE_ERROR_CODES）の場合は
    間隔を空けて自動的に再試行する。それ以外のエラーはそのまま例外にする。
    """
    last_error = None
    for attempt in range(1, MAX_API_RETRIES + 1):
        res = requests.get(url, params=params, timeout=timeout)
        payload = res.json()
        if 'error' not in payload:
            return payload
        error = payload['error']
        last_error = error
        code = error.get('code')
        if code in RETRYABLE_ERROR_CODES and attempt < MAX_API_RETRIES:
            print(f'一時的なMeta APIエラー（code={code}: {error.get("message")}）。'
                  f'{RETRY_WAIT_SECONDS}秒待って再試行します（{attempt}/{MAX_API_RETRIES}）')
            time.sleep(RETRY_WAIT_SECONDS)
            continue
        break
    raise RuntimeError(f"Meta APIエラー: {last_error.get('message')} (code={last_error.get('code')})")


# Google Sheets API側の一時的なエラー（過負荷・メンテナンス等）で、時間を置けば解消することが多いもの。
# 429: レート制限 / 500: サーバー側エラー / 502,503,504: 一時的な応答不能
SHEETS_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_SHEETS_RETRIES = 3
SHEETS_RETRY_WAIT_SECONDS = 20


def call_with_retry(func, *args, **kwargs):
    """
    gspread（Google Sheets API）呼び出しが一時的なエラー（503など）で失敗した場合、
    間隔を空けて自動的に再試行する。Meta API側のgraph_api_getと同じ考え方。
    例: call_with_retry(ws.append_rows, rows, value_input_option='USER_ENTERED')
    """
    last_error = None
    for attempt in range(1, MAX_SHEETS_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            status = getattr(e.response, 'status_code', None)
            if status not in SHEETS_RETRYABLE_STATUS_CODES or attempt == MAX_SHEETS_RETRIES:
                raise
            last_error = e
            print(f'一時的なGoogle Sheets APIエラー（status={status}）。'
                  f'{SHEETS_RETRY_WAIT_SECONDS}秒待って再試行します（{attempt}/{MAX_SHEETS_RETRIES}）: {e}')
            time.sleep(SHEETS_RETRY_WAIT_SECONDS)
    raise last_error


# バックフィルなどで一度に大量の行を書き込むと、リクエスト自体が大きくなり一時エラーが
# 起きやすくなる。そのためbatch_size件ずつに分けて書き込む（各バッチはcall_with_retryで
# リトライ済みなので、1バッチが失敗しても最初からやり直しにはならない）。
SHEETS_WRITE_BATCH_SIZE = 500


def append_rows_batched(ws, rows, batch_size=SHEETS_WRITE_BATCH_SIZE):
    total = len(rows)
    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        call_with_retry(ws.append_rows, batch, value_input_option='USER_ENTERED')
        print(f'  書き込み中: {min(i + batch_size, total)}/{total}行')


def update_rows_batched(ws, headers, out_rows, batch_size=SHEETS_WRITE_BATCH_SIZE):
    """upsert_rows用。シート全体（ヘッダー+全行）を書き直す際、行範囲を分けて書き込む。"""
    call_with_retry(ws.update, range_name='A1', values=[headers])
    total = len(out_rows)
    for i in range(0, total, batch_size):
        batch = out_rows[i:i + batch_size]
        start_a1 = gspread.utils.rowcol_to_a1(2 + i, 1)
        call_with_retry(ws.update, range_name=start_a1, values=batch)
        print(f'  月次リーチ書き込み中: {min(i + batch_size, total)}/{total}行')

HEADERS = [
    '日付', 'キャンペーン名', '広告セット名', '広告名',
    'サムネイル', 'サムネイルURL', '広告画像', '広告画像URL', '動画URL',
    '配信金額', 'IMP数', 'CPM', 'CTR(%)', 'クリック数', 'CPC',
    'プロフィール流入数', 'プロフィール流入単価', 'フォロー率', 'フォロー数', 'フォロワー獲得単価',
    'CVR(%)', 'CV', 'CPA', '購入金額', 'ROAS',
    '配信目的分類',  # 26列目。Meta側のcampaign objectiveから自動分類（獲得系/認知・トラフィック系/その他）
    'Meta配信目的(生データ)',  # 27列目。objectiveの生の値（例: OUTCOME_AWARENESS）。案件ごとに独自の分類をする際の元データ
    'クリエイティブ種別',  # 28列目。「動画」「静止画」。video_idの有無で判定するため、動画URL(source)の
                        # 取得が権限不足(code=10)等で失敗しても正しく「動画」と判定できる
]

# 月次リーチ（別シート）のヘッダー。「対象年月」を持つ（日付ではない）。
# リーチ数は日をまたいで単純合算すると同じ人を複数回カウットしてしまい実際のユニークリーチより
# 多く出るため、Metaに毎回「その月の1日〜対象日」をまるごとtime_rangeとして問い合わせて
# 月単位のユニークリーチを取得し、行を追記ではなく上書き（アップサート）する運用にしている。
MONTHLY_AGE_HEADERS = [
    '対象年月', 'キャンペーン名', '年齢層',
    '配信金額', 'IMP数', 'リーチ数', 'クリック数', 'CTR(%)', 'CPC', 'リーチ1000人あたり単価',
    '配信目的分類', 'Meta配信目的(生データ)',
]

# 月次×広告単位×年齢層（別シート）のヘッダー。年齢層ごとの「上位/下位クリエイティブ」表示のために
# level='ad' + breakdowns=age で取得する。月次リーチと同様、月をまるごとtime_rangeにして
# アップサートする（日次にすると広告数×年齢数×日数で行数が爆発的に増えるため月次限定とする）。
MONTHLY_AD_AGE_HEADERS = [
    '対象年月', 'キャンペーン名', '広告セット名', '広告名', '年齢層',
    'サムネイルURL', '配信金額', 'IMP数', 'クリック数', 'CTR(%)', 'CPC',
    '配信目的分類', 'Meta配信目的(生データ)',
]

# Instagramフォロワー数（別シート）のヘッダー。日付ごとに1行。
# 「フォロワー数」は推定の総数（下記fetch_instagram_daily_follower_increaseの説明を参照。
# 現在時点のスナップショットから日次増減を差し引いて逆算した推定値であり、Meta側が返す確定値ではない）。
# 「前日比増減」がMeta Instagram Insights APIから直接取得した、その日の新規フォロワー増加数（確定値）。
INSTAGRAM_FOLLOWER_HEADERS = ['日付', 'フォロワー数(推定)', '前日比増減']

# action_type の判定に使う優先順位リスト（完全一致）。
# Metaは同じ1件のコンバージョンを omni_purchase / purchase / offsite_conversion.fb_pixel_purchase
# など複数の集計方式で重複して返すため、合算はせず「最初に見つかった1種類だけ」を採用する。
ACTION_MATCHERS = {
    'purchase': ['omni_purchase', 'purchase', 'offsite_conversion.fb_pixel_purchase'],
    'profile_visit': ['profile_visit'],
    'follow': ['follow'],
}

# Metaのcampaign objectiveから「獲得系」「認知・トラフィック系」への分類。
# 2026年7月時点でプロフィール流入・フォロー数はMarketing APIで未提供のため、
# それらは別途「手動指標」シートで補う運用とする（Dashboard.gs側で読み込み・合算）。
ACQUISITION_OBJECTIVES = {
    'OUTCOME_SALES', 'OUTCOME_LEADS', 'CONVERSIONS', 'PRODUCT_CATALOG_SALES',
    'LEAD_GENERATION', 'STORE_VISITS',
}
AWARENESS_TRAFFIC_OBJECTIVES = {
    'OUTCOME_AWARENESS', 'OUTCOME_TRAFFIC', 'OUTCOME_ENGAGEMENT', 'OUTCOME_APP_PROMOTION',
    'BRAND_AWARENESS', 'REACH', 'LINK_CLICKS', 'POST_ENGAGEMENT', 'VIDEO_VIEWS',
    'MESSAGES', 'APP_INSTALLS',
}


def classify_objective(objective):
    if objective in ACQUISITION_OBJECTIVES:
        return '獲得系'
    if objective in AWARENESS_TRAFFIC_OBJECTIVES:
        return '認知・トラフィック系'
    return 'その他'


def fetch_campaign_objective(campaign_id, token, cache):
    """
    戻り値は {'category': '獲得系'などの分類, 'raw': 'OUTCOME_AWARENESS'などの生の値} の辞書。
    生の値も保持しておくことで、案件ごとに独自の分類（例: リーチ目的/流入目的）を後から
    スプレッドシート側・ダッシュボード側で組み立て直せるようにする。
    """
    if not campaign_id:
        return {'category': 'その他', 'raw': ''}
    if campaign_id in cache:
        return cache[campaign_id]
    result = {'category': 'その他', 'raw': ''}
    try:
        data = graph_api_get(
            f'https://graph.facebook.com/{API_VERSION}/{campaign_id}',
            {'fields': 'objective', 'access_token': token}, timeout=30,
        )
        raw = data.get('objective', '') or ''
        result = {'category': classify_objective(raw), 'raw': raw}
    except (requests.RequestException, RuntimeError) as e:
        print(f'配信目的の取得に失敗（campaign_id={campaign_id}）: {e}')
    cache[campaign_id] = result
    return result


def get_spreadsheet():
    creds_info = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(os.environ['SPREADSHEET_ID'])


def get_or_create_sheet(sh, sheet_name, headers):
    """指定したシート名・ヘッダー構成でワークシートを取得（無ければ作成）し、ヘッダー行を整える。"""
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=1000, cols=len(headers))

    if ws.col_count < len(headers):
        # シートの物理的な列数がheadersの数より少ない場合、書き込み前に列を追加しておく
        # （足りないまま書き込むと "exceeds grid limits" エラーになる）
        call_with_retry(ws.add_cols, len(headers) - ws.col_count)

    existing_header = call_with_retry(ws.row_values, 1) if ws.row_count > 0 else []
    if not existing_header:
        call_with_retry(ws.update, range_name='A1', values=[headers])
    elif len(existing_header) < len(headers):
        # 以前のバージョンで作られたシートに新しい列が増えている場合、
        # 既存のヘッダーはそのままに、不足分だけを末尾に追記する（過去データの列はズレない）
        missing = headers[len(existing_header):]
        start_col_letter = gspread.utils.rowcol_to_a1(1, len(existing_header) + 1)
        call_with_retry(ws.update, range_name=start_col_letter, values=[missing])
    return ws


def upsert_rows(ws, headers, new_rows, key_indices):
    """new_rowsをkey_indices列の値をキーとして反映する（同じキーの行があれば上書き、無ければ追加）。
    リーチ数のように「日ごとに単純合算すると重複カウントになる」指標を月単位で管理するために使う。
    件数がそれほど多くないシート（月次サマリー）向けの実装として、シート全体を読み直して
    まとめて書き戻す（全行末尾への追記ではなく、シート内容そのものを更新する点に注意）。"""
    existing = call_with_retry(ws.get_all_values)
    data_rows = existing[1:] if existing else []

    merged = {}
    for row in data_rows:
        padded = row + [''] * (len(headers) - len(row))
        key = tuple(str(padded[i]) for i in key_indices)
        merged[key] = padded
    for row in new_rows:
        key = tuple(str(row[i]) for i in key_indices)
        merged[key] = row

    out_rows = [merged[k] for k in sorted(merged.keys())]
    call_with_retry(ws.clear)
    update_rows_batched(ws, headers, out_rows)
    return len(out_rows)


def get_last_date(ws):
    col = call_with_retry(ws.col_values, 1)[1:]  # ヘッダーを除く
    dates = [d for d in (parse_date_safe(v) for v in col) if d]
    return max(dates) if dates else None


def parse_date_safe(v):
    try:
        return date.fromisoformat(v[:10])
    except (ValueError, TypeError):
        return None


# 1回のAPI呼び出しで取得する日数。データ量が多いアカウントだと120日でもエラーになることが
# 分かったため、内部的にこの日数ごとへ自動分割してから順番に取得する。
INSIGHTS_CHUNK_DAYS = 30


def fetch_insights(account_id, token, since, until):
    """指定した since〜until を丸ごと受け取り、内部でINSIGHTS_CHUNK_DAYSごとに自動分割して取得する。"""
    all_rows = []
    chunk_start = since
    while chunk_start <= until:
        chunk_end = min(chunk_start + timedelta(days=INSIGHTS_CHUNK_DAYS - 1), until)
        print(f'  取得中: {chunk_start} 〜 {chunk_end}')
        all_rows.extend(fetch_insights_chunk(account_id, token, chunk_start, chunk_end))
        chunk_start = chunk_end + timedelta(days=1)
    return all_rows


def fetch_insights_chunk(account_id, token, since, until):
    fields = ','.join([
        'ad_id', 'ad_name', 'adset_name', 'campaign_name', 'campaign_id',
        'spend', 'impressions', 'cpm',
        'inline_link_clicks',  # 広告マネージャの「リンククリック」と同じ定義（全クリックより狭い）
        'actions', 'action_values', 'date_start',
    ])
    url = f'https://graph.facebook.com/{API_VERSION}/{account_id}/insights'
    params = {
        'level': 'ad',
        'time_increment': 1,
        'time_range': json.dumps({'since': since.isoformat(), 'until': until.isoformat()}),
        'fields': fields,
        'limit': 500,
        'access_token': token,
    }
    rows = []
    while url:
        payload = graph_api_get(url, params)
        rows.extend(payload.get('data', []))
        paging = payload.get('paging', {})
        url = paging.get('next')
        params = None  # next URLに全パラメータが含まれるため以降は不要
    return rows


def month_start_(d):
    return date(d.year, d.month, 1)


def month_end_(d):
    if d.month == 12:
        return date(d.year, 12, 31)
    return date(d.year, d.month + 1, 1) - timedelta(days=1)


def months_between_(since, until):
    """since〜untilの範囲にかかる暦月を、各月の月初日として列挙する（例: 7/20〜8/5 → 7/1, 8/1）。"""
    cur = month_start_(since)
    while cur <= until:
        yield cur
        cur = (date(cur.year + 1, 1, 1) if cur.month == 12 else date(cur.year, cur.month + 1, 1))


def fetch_monthly_age_insights(account_id, token, since, until):
    """since〜untilにかかる暦月ごとに『月初〜対象日』をまるごとtime_rangeとして問い合わせ、
    月単位のユニークリーチを取得する（日次データの単純合算だと重複カウントになるため）。
    月が進行中でも構わない。翌日以降に対象月がもう一度取得されるたびに、より新しい
    『月初〜その時点のuntil』のデータで上書きされ、月末を過ぎれば結果的にその月の確定値になる。"""
    all_rows = []
    for m_start in months_between_(since, until):
        m_end = min(month_end_(m_start), until)
        month_label = f'{m_start.year}-{m_start.month:02d}'
        rows = fetch_monthly_age_insights_chunk(account_id, token, m_start, m_end)
        for r in rows:
            r['_month_label'] = month_label
        all_rows.extend(rows)
    return all_rows


def fetch_monthly_age_insights_chunk(account_id, token, since, until):
    fields = ','.join([
        'campaign_id', 'campaign_name',
        'spend', 'impressions', 'reach', 'inline_link_clicks',
    ])
    url = f'https://graph.facebook.com/{API_VERSION}/{account_id}/insights'
    params = {
        'level': 'campaign',
        # time_incrementを指定しない = since〜until全体を1つの集計期間として扱う
        # （これによりreachがその期間のユニークリーチになる。日次分割すると重複カウントになる）
        'breakdowns': 'age',
        'time_range': json.dumps({'since': since.isoformat(), 'until': until.isoformat()}),
        'fields': fields,
        'limit': 500,
        'access_token': token,
    }
    rows = []
    while url:
        payload = graph_api_get(url, params)
        rows.extend(payload.get('data', []))
        paging = payload.get('paging', {})
        url = paging.get('next')
        params = None
    return rows


def build_monthly_age_row(insight, objective_cache, token):
    spend = float(insight.get('spend', 0) or 0)
    impressions = float(insight.get('impressions', 0) or 0)
    reach = float(insight.get('reach', 0) or 0)
    clicks = float(insight.get('inline_link_clicks', 0) or 0)
    ctr = (clicks / impressions * 100) if impressions > 0 else 0.0
    cpc = (spend / clicks) if clicks > 0 else 0.0
    cpm_reach = (spend / reach * 1000) if reach > 0 else 0.0

    objective_info = fetch_campaign_objective(insight.get('campaign_id'), token, objective_cache)

    return [
        insight.get('_month_label', ''),
        insight.get('campaign_name', ''),
        insight.get('age', ''),
        spend, impressions, reach, clicks, ctr, cpc, cpm_reach,
        objective_info['category'], objective_info['raw'],
    ]


def fetch_creative(ad_id, token, cache):
    if ad_id in cache:
        return cache[ad_id]

    result = {'thumbnail_url': '', 'image_url': '', 'video_url': '', 'is_video': False}
    try:
        fields = 'creative{thumbnail_url,image_url,video_id,object_story_spec}'
        data = graph_api_get(
            f'https://graph.facebook.com/{API_VERSION}/{ad_id}',
            {'fields': fields, 'access_token': token}, timeout=30,
        )
        creative = data.get('creative', {}) or {}
        result['thumbnail_url'] = creative.get('thumbnail_url', '') or ''

        image_url = creative.get('image_url', '') or ''
        video_id = creative.get('video_id', '') or ''
        oss = creative.get('object_story_spec', {}) or {}
        link_data = oss.get('link_data', {}) or {}
        if not image_url:
            if link_data.get('picture'):
                image_url = link_data['picture']
            elif link_data.get('child_attachments'):
                image_url = link_data['child_attachments'][0].get('picture', '') or ''
        if not video_id and oss.get('video_data', {}).get('video_id'):
            video_id = oss['video_data']['video_id']

        result['image_url'] = image_url

        if video_id:
            # video_idが取れた時点で「動画クリエイティブである」ことは確定する。
            # 再生用ソースURL(source)の取得は権限不足（code=10）で失敗することがあるが、
            # その場合でも動画であること自体は変わらないため、is_videoはURL取得の成否に依存させない。
            result['is_video'] = True
            try:
                vdata = graph_api_get(
                    f'https://graph.facebook.com/{API_VERSION}/{video_id}',
                    {'fields': 'source', 'access_token': token}, timeout=30,
                )
                result['video_url'] = vdata.get('source', '') or ''
            except (requests.RequestException, RuntimeError) as e:
                print(f'動画URL取得に失敗（ad_id={ad_id}, video_id={video_id}）: {e}')
    except (requests.RequestException, RuntimeError) as e:
        print(f'クリエイティブ取得に失敗（ad_id={ad_id}）: {e}')

    cache[ad_id] = result
    return result


def fetch_monthly_ad_age_insights(account_id, token, since, until):
    """since〜untilにかかる暦月ごとに『月初〜対象日』をまるごとtime_rangeとして問い合わせ、
    広告単位×年齢層の月次実績を取得する（年齢別の「上位/下位クリエイティブ」表示用）。
    広告数×年齢数だけ行数が増えるため、日次ではなく月次限定で取得する。"""
    all_rows = []
    for m_start in months_between_(since, until):
        m_end = min(month_end_(m_start), until)
        month_label = f'{m_start.year}-{m_start.month:02d}'
        rows = fetch_monthly_ad_age_insights_chunk(account_id, token, m_start, m_end)
        for r in rows:
            r['_month_label'] = month_label
        all_rows.extend(rows)
    return all_rows


def fetch_monthly_ad_age_insights_chunk(account_id, token, since, until):
    fields = ','.join([
        'ad_id', 'ad_name', 'adset_name', 'campaign_id', 'campaign_name',
        'spend', 'impressions', 'inline_link_clicks',
    ])
    url = f'https://graph.facebook.com/{API_VERSION}/{account_id}/insights'
    params = {
        'level': 'ad',
        'breakdowns': 'age',
        'time_range': json.dumps({'since': since.isoformat(), 'until': until.isoformat()}),
        'fields': fields,
        'limit': 500,
        'access_token': token,
    }
    rows = []
    while url:
        payload = graph_api_get(url, params)
        rows.extend(payload.get('data', []))
        paging = payload.get('paging', {})
        url = paging.get('next')
        params = None
    return rows


def build_monthly_ad_age_row(insight, token, creative_cache, objective_cache):
    spend = float(insight.get('spend', 0) or 0)
    impressions = float(insight.get('impressions', 0) or 0)
    clicks = float(insight.get('inline_link_clicks', 0) or 0)
    ctr = (clicks / impressions * 100) if impressions > 0 else 0.0
    cpc = (spend / clicks) if clicks > 0 else 0.0

    creative = fetch_creative(insight.get('ad_id'), token, creative_cache)
    objective_info = fetch_campaign_objective(insight.get('campaign_id'), token, objective_cache)

    return [
        insight.get('_month_label', ''),
        insight.get('campaign_name', ''),
        insight.get('adset_name', ''),
        insight.get('ad_name', ''),
        insight.get('age', ''),
        creative['thumbnail_url'],
        spend, impressions, clicks, ctr, cpc,
        objective_info['category'], objective_info['raw'],
    ]


def pick_action_value(items, priority_types):
    """
    優先順位リストの中で最初に見つかった1種類の action_type の値だけを返す。
    Metaが同じコンバージョンを複数のaction_typeで重複して返すことがあるため、
    合算せずに1つだけ採用することで水増しを防ぐ。
    """
    values_by_type = {}
    for item in items or []:
        action_type = item.get('action_type', '') or ''
        try:
            values_by_type[action_type] = values_by_type.get(action_type, 0) + float(item.get('value', 0))
        except (TypeError, ValueError):
            pass
    for t in priority_types:
        if t in values_by_type:
            return values_by_type[t]
    return 0.0


def build_row(insight, token, creative_cache, objective_cache):
    spend = float(insight.get('spend', 0) or 0)
    impressions = float(insight.get('impressions', 0) or 0)
    clicks = float(insight.get('inline_link_clicks', 0) or 0)  # 広告マネージャの「リンククリック」相当
    cpm = float(insight.get('cpm', 0) or 0)
    ctr = (clicks / impressions * 100) if impressions > 0 else 0.0
    cpc = (spend / clicks) if clicks > 0 else 0.0

    purchase_count = pick_action_value(insight.get('actions'), ACTION_MATCHERS['purchase'])
    purchase_value = pick_action_value(insight.get('action_values'), ACTION_MATCHERS['purchase'])
    profile_visits = pick_action_value(insight.get('actions'), ACTION_MATCHERS['profile_visit'])
    follows = pick_action_value(insight.get('actions'), ACTION_MATCHERS['follow'])

    cvr = (purchase_count / clicks * 100) if clicks > 0 else ''
    cpa = (spend / purchase_count) if purchase_count > 0 else ''
    roas = (purchase_value / spend) if spend > 0 else ''
    profile_visit_cost = (spend / profile_visits) if profile_visits > 0 else ''
    follow_rate = (follows / profile_visits * 100) if profile_visits > 0 else ''
    follow_cost = (spend / follows) if follows > 0 else ''

    creative = fetch_creative(insight.get('ad_id'), token, creative_cache)
    objective_info = fetch_campaign_objective(insight.get('campaign_id'), token, objective_cache)

    return [
        insight.get('date_start', ''),
        insight.get('campaign_name', ''),
        insight.get('adset_name', ''),
        insight.get('ad_name', ''),
        '',  # サムネイル列（旧=IMAGE()式）。大量に蓄積するとスプレッドシート全体が重くなるため書き込みをやめた。
             # 実際に使うのは次のサムネイルURL列（プレーンテキスト）。必要ならこの列にセル単位で
             # =IMAGE(隣のURLセル) と手動入力すれば個別に画像表示できる。
        creative['thumbnail_url'],
        '',  # 広告画像列（旧=IMAGE()式）。理由は上と同じ。
        creative['image_url'],
        creative['video_url'],
        spend, impressions, cpm, ctr, clicks, cpc,
        profile_visits, profile_visit_cost, follow_rate, follows, follow_cost,
        cvr, purchase_count, cpa, purchase_value, roas,
        objective_info['category'], objective_info['raw'],
        '動画' if creative['is_video'] else '静止画',
    ]


# Instagram Insightsの日次系メトリクス（follower_count等）は、実行時点から遡れる期間に上限があり、
# 一般的には直近30日分程度までしか取得できない（それより古い日付をsinceに指定するとAPIエラーになる）。
# そのため取得開始日をこの範囲内に自動でクランプする。
INSTAGRAM_INSIGHTS_MAX_LOOKBACK_DAYS = 30


def date_to_unix_ts_(d):
    """dateをUTC 0時のUnixタイムスタンプに変換する（Instagram Insights APIのsince/untilが要求する形式）。"""
    return calendar.timegm(d.timetuple())


def fetch_instagram_daily_follower_increase(ig_user_id, token, since, until):
    """
    Instagram Insights API（metric=follower_count, period=day）から、日ごとの新規フォロワー
    増加数を取得する。Meta公式ドキュメントでは、period=dayでのfollower_countは「その日に
    新たにフォローしたユニークアカウント数」と定義されており、フォロワー総数のスナップショット
    ではなく増加数そのものである（＝「フォロワー増加数」を求める今回の目的に直接使える）。
    戻り値は {date: increase(int)} の辞書。
    """
    since_ts = date_to_unix_ts_(since)
    # untilは指定した時刻より前のデータを返す仕様のため、対象日を含めるために1日後ろにずらす。
    until_ts = date_to_unix_ts_(until + timedelta(days=1))
    payload = graph_api_get(
        f'https://graph.facebook.com/{API_VERSION}/{ig_user_id}/insights',
        {
            'metric': 'follower_count',
            'period': 'day',
            'since': since_ts,
            'until': until_ts,
            'access_token': token,
        },
        timeout=30,
    )
    result = {}
    for metric in payload.get('data', []):
        for v in metric.get('values', []):
            d = parse_date_safe(str(v.get('end_time', ''))[:10])
            if d:
                result[d] = int(v.get('value', 0) or 0)
    return result


def fetch_instagram_current_followers(ig_user_id, token):
    """現在時点のフォロワー総数（スナップショット）を取得する。"""
    data = graph_api_get(
        f'https://graph.facebook.com/{API_VERSION}/{ig_user_id}',
        {'fields': 'followers_count', 'access_token': token}, timeout=30,
    )
    return int(data.get('followers_count', 0) or 0)


def build_instagram_follower_rows(ig_user_id, token, since, until):
    """
    since〜until（両端含む）の各日について [日付, フォロワー数(推定), 前日比増減] の行を作る。
    「前日比増減」はfollower_count（確定値）そのもの。「フォロワー数(推定)」は、現在時点の
    実際の総数（フォロワー数の直接取得はスナップショットのみで過去日には遡れないため）から、
    直近の増減を1日ずつ差し引いて逆算した推定値であり、厳密な確定値ではない
    （today分の増減がまだ反映されていない可能性があるため、直近日ほど僅かな誤差の余地がある）。
    """
    increases = fetch_instagram_daily_follower_increase(ig_user_id, token, since, until)
    current_total = fetch_instagram_current_followers(ig_user_id, token)

    totals_by_date = {}
    running_total = current_total
    d = until
    while d >= since:
        totals_by_date[d] = running_total
        running_total -= increases.get(d, 0)
        d -= timedelta(days=1)

    rows = []
    d = since
    while d <= until:
        rows.append([d.isoformat(), totals_by_date.get(d, ''), increases.get(d, 0)])
        d += timedelta(days=1)
    return rows


def parse_flexible_date(s):
    """
    'YYYY-MM-DD' の他に 'YYYY/M/D' や 'YYYY.M.D' のようなゼロ埋めなし・区切り文字違いの
    入力ミスも許容して日付に変換する（workflow_dispatchの手入力欄は形式チェックがないため）。
    """
    normalized = s.strip().replace('/', '-').replace('.', '-')
    parts = normalized.split('-')
    if len(parts) != 3:
        raise ValueError(f'日付の形式を認識できません: "{s}"（例: 2023-07-04 の形式で入力してください）')
    try:
        year, month, day = (int(p) for p in parts)
        return date(year, month, day)
    except ValueError as e:
        raise ValueError(f'日付の形式を認識できません: "{s}"（例: 2023-07-04 の形式で入力してください）') from e


def main():
    token = os.environ['META_ACCESS_TOKEN']
    account_id = os.environ['META_AD_ACCOUNT_ID']

    # 広告単位×年齢層の月次取得はデータ量が大きい（広告数×年齢数）ため、必要な案件のみ
    # リポジトリ変数 ENABLE_AD_AGE_INSIGHTS=true を設定してオプトインする（デフォルトは無効）。
    enable_ad_age_insights = os.environ.get('ENABLE_AD_AGE_INSIGHTS', '').strip().lower() in ('1', 'true', 'yes')
    instagram_account_id = os.environ.get('INSTAGRAM_ACCOUNT_ID', '').strip()

    sh = get_spreadsheet()
    sheet_name = os.environ.get('SHEET_NAME', 'Meta広告データ')
    monthly_age_sheet_name = os.environ.get('MONTHLY_AGE_SHEET_NAME', '月次リーチ')
    monthly_ad_age_sheet_name = os.environ.get('MONTHLY_AD_AGE_SHEET_NAME', '月次年齢別広告実績')
    instagram_sheet_name = os.environ.get('INSTAGRAM_SHEET_NAME', 'Instagramフォロワー数')
    ws = get_or_create_sheet(sh, sheet_name, HEADERS)
    monthly_age_ws = get_or_create_sheet(sh, monthly_age_sheet_name, MONTHLY_AGE_HEADERS)
    monthly_ad_age_ws = get_or_create_sheet(sh, monthly_ad_age_sheet_name, MONTHLY_AD_AGE_HEADERS) if enable_ad_age_insights else None
    instagram_ws = get_or_create_sheet(sh, instagram_sheet_name, INSTAGRAM_FOLLOWER_HEADERS) if instagram_account_id else None

    since_env = os.environ.get('BACKFILL_SINCE', '').strip()
    until_env = os.environ.get('BACKFILL_UNTIL', '').strip()
    backfill_env = os.environ.get('BACKFILL_DAYS', '').strip()
    yesterday = date.today() - timedelta(days=1)  # 前日まで（当日分は数値未確定）

    if since_env and until_env:
        # 古い期間をピンポイントで指定して取得する（何回かに分けて過去へ遡る運用向け）。
        # シートの続きかどうかは見ず、指定範囲をそのまま取得するため、
        # 既に取得済みの日付と重複させないよう呼び出し側で範囲をずらして使うこと。
        since = parse_flexible_date(since_env)
        until = parse_flexible_date(until_env)
        print(f'期間指定バックフィル: {since} 〜 {until}')
    elif backfill_env:
        until = yesterday
        since = until - timedelta(days=int(backfill_env) - 1)
        print(f'日数指定バックフィル: {since} 〜 {until}')
    else:
        until = yesterday
        last_date = get_last_date(ws)
        if last_date:
            since = last_date + timedelta(days=1)
        else:
            since = until - timedelta(days=DEFAULT_BACKFILL_DAYS - 1)
            print(f'シートが空のため初期バックフィル: {since} 〜 {until}')

    if since > until:
        print(f'新規に取得すべき期間がありません（スキップ）: since={since} until={until}')
    else:
        range_days = (until - since).days + 1
        print(f'取得対象: {range_days}日分（{INSIGHTS_CHUNK_DAYS}日ずつ自動分割して取得します）')

        insights = fetch_insights(account_id, token, since, until)
        if not insights:
            print(f'取得対象期間にデータがありませんでした: {since} 〜 {until}')
        else:
            creative_cache = {}
            objective_cache = {}
            rows = [build_row(row, token, creative_cache, objective_cache) for row in insights]

            append_rows_batched(ws, rows)
            print(f'{len(rows)}行を追記しました（{since} 〜 {until}）')

            monthly_age_insights = fetch_monthly_age_insights(account_id, token, since, until)
            if monthly_age_insights:
                monthly_age_rows = [build_monthly_age_row(row, objective_cache, token) for row in monthly_age_insights]
                total = upsert_rows(monthly_age_ws, MONTHLY_AGE_HEADERS, monthly_age_rows, key_indices=(0, 1, 2))
                print(f'月次リーチ: 対象期間の{len(monthly_age_rows)}行を月単位のユニークリーチで上書きしました（シート全体は{total}行）')
            else:
                print(f'月次リーチ: 取得対象期間にデータがありませんでした: {since} 〜 {until}')

            if enable_ad_age_insights:
                monthly_ad_age_insights = fetch_monthly_ad_age_insights(account_id, token, since, until)
                if monthly_ad_age_insights:
                    monthly_ad_age_rows = [
                        build_monthly_ad_age_row(row, token, creative_cache, objective_cache)
                        for row in monthly_ad_age_insights
                    ]
                    total = upsert_rows(monthly_ad_age_ws, MONTHLY_AD_AGE_HEADERS, monthly_ad_age_rows, key_indices=(0, 1, 2, 3, 4))
                    print(f'月次年齢別広告実績: 対象期間の{len(monthly_ad_age_rows)}行を上書きしました（シート全体は{total}行）')
                else:
                    print(f'月次年齢別広告実績: 取得対象期間にデータがありませんでした: {since} 〜 {until}')

    # Instagramフォロワー数は、Meta広告の取得期間（BACKFILL_SINCE/UNTIL等）とは独立に、
    # 「シートの続きから前日まで」を基準に取得する（INSTAGRAM_ACCOUNT_IDが設定されている場合のみ）。
    if instagram_account_id:
        ig_until = date.today() - timedelta(days=1)
        ig_last_date = get_last_date(instagram_ws)
        if ig_last_date:
            ig_since = ig_last_date + timedelta(days=1)
        else:
            ig_since = ig_until - timedelta(days=DEFAULT_BACKFILL_DAYS - 1)
            print(f'Instagramフォロワー数: シートが空のため初期バックフィル: {ig_since} 〜 {ig_until}')

        earliest_available = ig_until - timedelta(days=INSTAGRAM_INSIGHTS_MAX_LOOKBACK_DAYS - 1)
        if ig_since < earliest_available:
            print(f'Instagramフォロワー数: {ig_since}〜{earliest_available - timedelta(days=1)}は'
                  f'API側の取得可能期間（直近{INSTAGRAM_INSIGHTS_MAX_LOOKBACK_DAYS}日）より古いためスキップします')
            ig_since = earliest_available

        if ig_since > ig_until:
            print('Instagramフォロワー数: 新規に取得すべき期間がありません（スキップ）')
        else:
            try:
                ig_rows = build_instagram_follower_rows(instagram_account_id, token, ig_since, ig_until)
                append_rows_batched(instagram_ws, ig_rows)
                print(f'Instagramフォロワー数: {len(ig_rows)}日分を追記しました（{ig_since} 〜 {ig_until}）')
            except (requests.RequestException, RuntimeError) as e:
                print(f'Instagramフォロワー数の取得に失敗しました（後続処理には影響しません）: {e}')
    else:
        print('INSTAGRAM_ACCOUNT_ID が未設定のため、Instagramフォロワー数の取得をスキップします')


if __name__ == '__main__':
    main()
