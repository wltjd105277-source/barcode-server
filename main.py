from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import StreamingResponse, JSONResponse
import uvicorn
import pandas as pd
import io, os, httpx
from urllib.parse import quote
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────
# 이카운트 ERP API 설정 (.env에서 로드)
# ──────────────────────────────────────────────
ECOUNT_CONFIG = {
    "COM_CODE":     os.getenv("ECOUNT_COM_CODE", ""),
    "USER_ID":      os.getenv("ECOUNT_USER_ID", ""),
    "API_CERT_KEY": os.getenv("ECOUNT_API_KEY", ""),
    "ZONE":         os.getenv("ECOUNT_ZONE", "CD"),
    "LAN_TYPE":     "ko-KR",
    "CUST":         os.getenv("ECOUNT_CUST", ""),
    "WH_CD":        os.getenv("ECOUNT_WH_CD", ""),
}


async def get_ecount_session() -> tuple[str, str]:
    """이카운트 세션 ID 발급 (Zone API → OAPILogin 2단계)"""
    async with httpx.AsyncClient(timeout=30) as client:
        # ── 1단계: Zone API로 실제 Zone 조회 ──────────────
        zone_url = "https://oapi.ecount.com/OAPI/V2/Zone"
        zone_resp = await client.post(zone_url, json={"COM_CODE": ECOUNT_CONFIG["COM_CODE"]})
        zone_data = zone_resp.json()
        print(f"📡 Zone API 응답: {zone_data}")
        if str(zone_data.get("Status")) != "200" or not zone_data.get("Data"):
            raise Exception(f"Zone 조회 실패: {zone_data}")
        zone = zone_data["Data"]["ZONE"]          # 예: "CD"
        print(f"✅ Zone 확인: {zone}")

        # ── 2단계: OAPILogin으로 세션 발급 ────────────────
        login_url = f"https://oapi{zone.lower()}.ecount.com/OAPI/V2/OAPILogin"
        login_payload = {
            "COM_CODE":     ECOUNT_CONFIG["COM_CODE"],
            "USER_ID":      ECOUNT_CONFIG["USER_ID"].upper(),
            "API_CERT_KEY": ECOUNT_CONFIG["API_CERT_KEY"],
            "LAN_TYPE":     ECOUNT_CONFIG["LAN_TYPE"],
            "ZONE":         zone,
        }
        login_resp = await client.post(login_url, json=login_payload)
        login_data = login_resp.json()
        print(f"🔑 Login API 응답: {login_data}")

        # 오류 체크 (Status=200이지만 Error가 있을 수 있음)
        if login_data.get("Error"):
            raise Exception(f"이카운트 로그인 실패: {login_data['Error']}")
        if str(login_data.get("Status")) != "200":
            raise Exception(f"이카운트 로그인 실패: {login_data}")

        session_id = login_data["Data"]["Datas"]["SESSION_ID"]
        print(f"✅ 세션 발급 성공: {session_id[:10]}...")
        return session_id, zone

app = FastAPI()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

MASTER_PATH = os.path.join(BASE_DIR, "master_data.xlsx")


# ──────────────────────────────────────────────
# 마스터 데이터 로드 헬퍼
# ──────────────────────────────────────────────
def load_master():
    """master_data.xlsx에서 두 가지 매핑을 로드한다."""
    barcode_to_code = {}  # PO용: 바코드 → 상품코드
    code_to_barcode = {}  # 주문서용: 품목코드 → lineup11 바코드

    if not os.path.exists(MASTER_PATH):
        print(f"❌ master_data.xlsx 없음: {MASTER_PATH}")
        return barcode_to_code, code_to_barcode

    xl = pd.ExcelFile(MASTER_PATH)

    # PO 매핑 (시트: PO매핑 또는 단일 시트)
    po_sheet = "PO매핑" if "PO매핑" in xl.sheet_names else xl.sheet_names[0]
    df_po = pd.read_excel(MASTER_PATH, sheet_name=po_sheet, dtype=str)
    df_po.columns = df_po.columns.str.strip()
    if "바코드" in df_po.columns and "상품코드" in df_po.columns:
        df_po["바코드"]   = df_po["바코드"].fillna("").str.strip().str.replace(r"\.0$", "", regex=True)
        df_po["상품코드"] = df_po["상품코드"].fillna("").str.strip()
        barcode_to_code = {
            row["바코드"]: row["상품코드"]
            for _, row in df_po.iterrows()
            if row["바코드"]
        }
        print(f"✅ PO 매핑 로드: {len(barcode_to_code)}개")

    # 주문서 매핑 (시트: 주문서매핑)
    if "주문서매핑" in xl.sheet_names:
        df_ord = pd.read_excel(MASTER_PATH, sheet_name="주문서매핑", dtype=str)
        df_ord.columns = df_ord.columns.str.strip()
        if "품목코드" in df_ord.columns and "lineup11 바코드" in df_ord.columns:
            df_ord["품목코드"]        = df_ord["품목코드"].fillna("").str.strip()
            df_ord["lineup11 바코드"] = df_ord["lineup11 바코드"].fillna("").str.strip().str.replace(r"\.0$", "", regex=True)
            code_to_barcode = {
                row["품목코드"]: row["lineup11 바코드"]
                for _, row in df_ord.iterrows()
                if row["품목코드"]
            }
            with_bc = sum(1 for v in code_to_barcode.values() if v)
            print(f"✅ 주문서 매핑 로드: {len(code_to_barcode)}개 (바코드 있음: {with_bc}개)")

    return barcode_to_code, code_to_barcode


# ──────────────────────────────────────────────
# 라우트
# ──────────────────────────────────────────────
@app.get("/")
async def home(request: Request):
    return templates.TemplateResponse(
        request=request, name="index.html", context={"request": request}
    )


@app.get("/api/master-info")
async def master_info():
    """마스터 데이터 현황 반환"""
    barcode_to_code, code_to_barcode = load_master()
    return {
        "po_count":           len(barcode_to_code),
        "order_count":        len(code_to_barcode),
        "order_with_barcode": sum(1 for v in code_to_barcode.values() if v),
    }


# ── PO 파일 처리 ──────────────────────────────
@app.post("/api/process-po")
async def process_po(
    file:       UploadFile = File(...),
    barcodeCol: str        = Form("상품바코드"),
    codeCol:    str        = Form("상품코드"),
):
    print(f"\n--- 🚀 PO [{file.filename}] 시작 ---")
    barcode_to_code, _ = load_master()

    contents = await file.read()
    df = pd.read_excel(io.BytesIO(contents), dtype=str)
    df.columns = df.columns.str.strip()

    target_bc = barcodeCol.strip()
    if target_bc not in df.columns:
        print(f"❌ '{target_bc}' 열 없음. 현재 열: {list(df.columns)}")
        return JSONResponse(status_code=400, content={"detail": f"'{target_bc}' 열이 없습니다."})

    df[target_bc] = df[target_bc].fillna("").str.strip().str.replace(r"\.0$", "", regex=True)
    codes = df[target_bc].map(lambda bc: barcode_to_code.get(bc, ""))

    if codeCol in df.columns:
        df[codeCol] = codes
    else:
        idx = df.columns.get_loc(target_bc) + 1
        df.insert(idx, codeCol, codes)

    total   = int((df[target_bc] != "").sum())
    matched = int(((df[target_bc] != "") & (df[codeCol] != "")).sum())
    missed  = total - matched
    print(f"📊 PO 결과 - 전체:{total} 성공:{matched} 실패:{missed}")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    output.seek(0)

    safe_name = quote(file.filename.replace(".xlsx", "_상품코드.xlsx").replace(".xls", "_상품코드.xlsx"))
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition":          f"attachment; filename*=UTF-8''{safe_name}",
            "X-Total":                      str(total),
            "X-Matched":                    str(matched),
            "X-Missed":                     str(missed),
            "Access-Control-Expose-Headers": "X-Total, X-Matched, X-Missed",
        },
    )


# ── 주문서 처리 ───────────────────────────────
@app.post("/api/process-order")
async def process_order(
    file:       UploadFile = File(...),
    codeCol:    str        = Form("품목코드"),
    barcodeCol: str        = Form("lineup11 바코드"),
    headerRow:  int        = Form(1),
):
    print(f"\n--- 🚀 주문서 [{file.filename}] 시작 ---")
    _, code_to_barcode = load_master()

    contents  = await file.read()
    header_idx = headerRow - 1
    df = pd.read_excel(io.BytesIO(contents), header=header_idx, dtype=str)
    df.columns = df.columns.str.strip()

    target_code = codeCol.strip()
    target_bc   = barcodeCol.strip()

    if target_code not in df.columns:
        print(f"❌ '{target_code}' 열 없음. 현재 열: {list(df.columns)}")
        return JSONResponse(status_code=400, content={"detail": f"'{target_code}' 열이 없습니다."})

    df[target_code] = df[target_code].fillna("").str.strip()
    barcodes = df[target_code].map(lambda c: code_to_barcode.get(c, ""))

    if target_bc in df.columns:
        df[target_bc] = barcodes
    else:
        idx = df.columns.get_loc(target_code) + 1
        df.insert(idx, target_bc, barcodes)

    total   = int((df[target_code] != "").sum())
    matched = int(((df[target_code] != "") & (df[target_bc] != "")).sum())
    missed  = total - matched
    print(f"📊 주문서 결과 - 전체:{total} 성공:{matched} 실패:{missed}")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    output.seek(0)

    safe_name = quote(file.filename.replace(".xlsx", "_바코드.xlsx").replace(".xls", "_바코드.xlsx"))
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition":          f"attachment; filename*=UTF-8''{safe_name}",
            "X-Total":                      str(total),
            "X-Matched":                    str(matched),
            "X-Missed":                     str(missed),
            "Access-Control-Expose-Headers": "X-Total, X-Matched, X-Missed",
        },
    )


# ── 이카운트 판매 전표 등록 ─────────────────────
@app.post("/api/send-to-ecount")
async def send_to_ecount(
    file:       UploadFile = File(...),
    barcodeCol: str        = Form("상품바코드"),
    codeCol:    str        = Form("상품코드"),
):
    print(f"\n--- 📤 이카운트 전송 [{file.filename}] 시작 ---")
    barcode_to_code, _ = load_master()

    contents = await file.read()
    df = pd.read_excel(io.BytesIO(contents), dtype=str)
    df.columns = df.columns.str.strip()
    df = df.fillna("")

    target_bc = barcodeCol.strip()
    if target_bc not in df.columns:
        return JSONResponse(status_code=400, content={"detail": f"'{target_bc}' 열이 없습니다."})

    # 바코드 → 상품코드 변환
    df[target_bc] = df[target_bc].str.strip().str.replace(r"\.0$", "", regex=True)
    df[codeCol]   = df[target_bc].map(lambda bc: barcode_to_code.get(bc, ""))

    # 확정수량이 있는 행만 (0이 아닌 것)
    qty_col = "확정수량" if "확정수량" in df.columns else "발주수량"
    df[qty_col] = df[qty_col].str.strip()
    valid = df[(df[codeCol] != "") & (df[qty_col] != "") & (df[qty_col] != "0")].copy()

    if valid.empty:
        return JSONResponse(status_code=400, content={"detail": "전송할 유효한 데이터가 없습니다."})

    # 판매일자 결정 (입고예정일 또는 오늘)
    def parse_date(val):
        for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"]:
            try:
                return datetime.strptime(str(val)[:10], fmt).strftime("%Y%m%d")
            except:
                pass
        return datetime.today().strftime("%Y%m%d")

    io_date = datetime.today().strftime("%Y%m%d")
    doc_no  = valid["발주번호"].iloc[0] if "발주번호" in valid.columns else ""

    # BulkDatas 리스트 구성
    bulk_list = []
    for i, (_, row) in enumerate(valid.iterrows(), 1):
        qty = row[qty_col].replace(",", "").strip()
        price     = row.get("매입가", "").replace(",", "").strip()
        supply    = row.get("공급가", "").replace(",", "").strip()
        vat       = row.get("부가세", "").replace(",", "").strip()

        bulk_list.append({"BulkDatas": {
            "UPLOAD_SER_NO": "1",
            "IO_DATE":       io_date,
            "CUST":          ECOUNT_CONFIG["CUST"],
            "WH_CD":         ECOUNT_CONFIG["WH_CD"],
            "DOC_NO":        doc_no,
            "PROD_CD":       row[codeCol],
            "PROD_DES":      row.get("상품이름", ""),
            "QTY":           qty,
            "PRICE":         price,
            "SUPPLY_AMT":    supply,
            "VAT_AMT":       vat,
            "REMARKS":       f"쿠팡 발주 {doc_no}",
        }})

    # 세션 발급 및 전송
    try:
        session_id, zone = await get_ecount_session()
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"이카운트 로그인 실패: {str(e)}"})

    sale_url = f"https://oapi{zone.lower()}.ecount.com/OAPI/V2/Sale/SaveSale?SESSION_ID={session_id}"

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(sale_url, json={"SaleList": bulk_list})
        result = resp.json()

    print(f"📨 이카운트 응답: {result}")

    status   = result.get("Status")
    data     = result.get("Data", {})
    success  = data.get("SuccessCnt", 0)
    fail     = data.get("FailCnt", 0)
    slip_nos = data.get("SlipNos", [])
    errors   = []
    for rd in (data.get("ResultDetails") or []):
        if not rd.get("IsSuccess"):
            errors.append(rd.get("TotalError", ""))

    return JSONResponse(content={
        "status":    status,
        "total":     len(bulk_list),
        "success":   success,
        "fail":      fail,
        "slip_nos":  slip_nos,
        "errors":    errors,
        "unmatched": int((df[codeCol] == "").sum()),
    })


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
