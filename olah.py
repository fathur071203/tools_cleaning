import argparse
import re
from pathlib import Path
import pandas as pd
import sys
try:
    from rapidfuzz import process, fuzz
    _HAS_RAPIDFUZZ = True
except Exception:
    from difflib import SequenceMatcher
    _HAS_RAPIDFUZZ = False
    print("Warning: rapidfuzz not installed; falling back to difflib (install rapidfuzz for better fuzzy matching).")

# --- KONFIGURASI DAN DATA ---

_PT_PREFIX_RE = re.compile(
    r"^\s*p\s*\.?\s*t\s*\.?\s*[\.,;:/\\\-]*\s*", re.IGNORECASE
)
_WHITESPACE_RE = re.compile(r"\s+")
_KOPERASI_RE = re.compile(r"\bkoperasi\b", re.IGNORECASE)

# --- DATA KUPVA BB (Format: Nama: Sandi) ---
DATA_KUPVA = {
    "Able Exchange": "777248003",
    "Alfa Valasindo": "777248021",
    "Allinson Money Changer": "777248023",
    "Alvi Artha Valutama": "777249482",
    "Amartha Valasindo": "777248027",
    "Ananda Surya Perkasa": "777249421",
    "Wira Maju Kencana": "777249205",
    "Andalan Valas Berkah Sentosa": "777249433",
    "Anggrek Valasindo": "777248032",
    "Antarartha Benua": "777248034",
    "Anugrah Mega Perkasa": "777248037",
    "Anugerah Semesta Valasindo": "777249071",
    "Anugrah Tangguh Mandiri": "777249006",
    "Anugrah Bestari Curency": "777249105",
    "Arian Valasindo": "777248973",
    "Arta Alexindo Utama": "777248047",
    "Arta Biena Ventura": "777248049",
    "Arta Valasindo": "777248051",
    "Artha Corrine Ganesha": "777249012",
    "Artha Gemilang Inter Valuta": "777248060",
    "Artha Kencana Valasindo": "777248063",
    "Artha Valasindo Mandiri": "777248947",
    "Arthamas Citra Mandiri": "777248070",
    "Asba Jaya Exchange": "777249431",
    "Asia Ritz Valutamas": "777248076",
    "Astika Bina Artha": "777249126",
    "Ayu Masagung": "777952443",
    "Azkha Berkah Valasindo": "777249422",
    "Bahari Buana Citra": "777248082",
    "Barokah Artha Kelola": "777249507",
    "Berkah Langgeng Abadi": "777248108",
    "Berkah Sri Valasindo": "777249092",
    "Berkah Sukses Sejahtera": "777248111",
    "Berkah Widjojo Valasindo": "777249054",
    "Bersama Intitama Valasindo": "777248118",
    "Bharata Millenium Pratama": "777248120",
    "Bhumi Graha Valas": "777249159",
    "Binavalasindo Dolarasia Sejahtera Utama": "777248943",
    "Bintang Fawwaz": "777248937",
    "Bintang Valas Abadi": "777249285",
    "Bogor Suci Sejahtera": "777249093",
    "Bumi Kapital Makmur": "777249055",
    "Cahaya Adi Sukses Hutama": "777248146",
    "Cahaya Nabila Valutama": "777249199",
    "Cahaya Nagamas Abadi": "777248149",
    "Cahaya Pratama Internasional": "777249080",
    "Cahaya Utama Dwivalas": "777248151",
    "Cahaya Valasindo Prima": "777248152",
    "Capital Currency Changer": "777248158",
    "Chandraprima Abadi Makmur": "777249424",
    "Cideng Mas": "777248176",
    "Cipta Indo Perkasa": "777248936",
    "Cipta Sentral Moneta": "777249508",
    "Citra Valasindo": "777248189",
    "D8 Valasindo": "777248886",
    "Daha Mulia Valasindo": "777249016",
    "Damai Valas Artha Sejahtera": "777249063",
    "Danau Hijau Abadi": "777249240",
    "Dawei Valuta Inter Nusa": "777249064",
    "Defidatama Bintang Artha": "777248203",
    "Delta Artha Mas Prima": "777248209",
    "Dinamika Arta Jaya": "777249160",
    "Dinidina SetyaRahma": "777248225",
    "Ditaraya Rizqi Bahagia": "777249483",
    "Do It Valasindo": "777248227",
    "Dolarindo Intervalas Primatama": "777248229",
    "Dolarindo Intravalas Primatama": "777953298",
    "Dolartime Premium Forexindo": "777249032",
    "Dollar Mart": "777248231",
    "Dua Putra Valutama": "777248235",
    "Dunia Multi Valasindo": "777249137",
    "Duta Gunung Kawi": "777248240",
    "Edvindo Mulia Artha": "777248249",
    "Eka Jaya Valasmas": "777248250",
    "Elok Valuta Indonesia": "777249276",
    "Emerald Valasindo": "777248252",
    "Emir Mulia Valas": "777249498",
    "Empress Agensiatama": "777248255",
    "Eraska Valutama": "777248258",
    "Fysstan": "777248270",
    "Gajah Mungkur Valuta": "777248274",
    "Gandaria Sukses Mandiri": "777249206",
    "Garuda Mandiri Valas": "777249286",
    "Garuda Valasindo": "777248278",
    "Garudamulti Valasindo": "777248279",
    "Gede Agung Bagus Jaya": "777249249",
    "Gemilang Nasional Rezeki Inter Valas": "777248285",
    "Gerizimindo Money Changer": "777248288",
    "Gita Wiraswasta Valasa": "777248294",
    "Global Artha Jaya": "777248884",
    "Global Mulia Valas": "777249091",
    "Global Saliartha Intivaluta": "777249180",
    "Golden City Graha": "777248301",
    "Gunsa Valas Utama": "777248313",
    "Gunung Batu Perisai": "777249442",
    "Harvestama Valuta Sejahtera": "777249170",
    "Homeeah Valas Inti": "777249182",
    "Indo Dollar Valuta Asia": "777248336",
    "Indonesia Central Valutamas": "777248338",
    "Indorate Prima Javalas": "777249437",
    "Indovalas Mitrautama": "777248341",
    "Inter Asia Konverta": "777248344",
    "International Valas": "777248347",
    "Jaya Valuta Mas": "777249173",
    "Kaka Valas Andalan": "777248375",
    "Karunia Restu Bunda": "777248381",
    "Karya Utama Valasindo": "777248385",
    "Kelroul CitraNusa Persada": "777248389",
    "Kenanga Kharisma Adimulia": "777248391",
    "Kevin Valasindo": "777248396",
    "Kharisma Valas Indonesia": "777248399",
    "Kie Valas Succesindo": "777248883",
    "Kindo Multivalas Jaya": "777248402",
    "King Valas": "777248403",
    "Kokas88 Valasindo": "777249038",
    "Kudamas Vasing": "777248407",
    "Ladang Mandiri Perkasa": "777248414",
    "Laksana Indonesia Mandiri": "777248416",
    "Lima Asa": "777248419",
    "Limindo Valas Utama": "777248421",
    "Logamindo Dwicipta": "777248428",
    "Lumbung Berkah": "777248433",
    "Lumbung Valasindo Nusantara": "777249007",
    "Makmur Berikat Sukses": "777249204",
    "Matauang Multivalas Mandiri": "777248450",
    "Media Artha Sukses Sarana": "777248452",
    "Mekar Setia Abadi": "777249445",
    "Bhaktishakti Ganda Intiharmoni": "777248459",
    "Metro Jala Masino": "777248468",
    "Midas Xchange Valasia": "777248978",
    "Mitra Andalan Valasindo": "777248962",
    "Mitra Niaga Artha Valasindo": "777248474",
    "Mitra Tunggal Valasindo": "777248476",
    "Mix Valasindo utama": "777249450",
    "Monies Mahakarya": "777248483",
    "Muchad Artha Jaya": "777248487",
    "Mufee Valasindo": "777248990",
    "Mulia Abadi Valas": "777249208",
    "Mulia Trinitas Valasindo": "777249106",
    "Multi Arthajaya Mandiri": "777249263",
    "Multi Kawan Valasindo": "777248496",
    "Niki Valasarta Jaya": "777248513",
    "Nusa Agung Valasindo": "777249221",
    "Nusa Multi Valas": "777248519",
    "One Valas": "777248524",
    "Oriental Pacific": "777248526",
    "Pamada Indotama": "777248532",
    "Paramas Murni": "777248538",
    "Peniti Valasindo": "777953293",
    "Permata Abadi Valasindo": "777249003",
    "Permata Tanet Valasindo": "777248547",
    "Permata Valasindo": "777248548",
    "Piti Pili": "777248556",
    "Platinum Exchange": "777248557",
    "Point Valuta Asing": "777249088",
    "Porto Valas": "777248563",
    "Pratama Artha Valas": "777249040",
    "Prima Artha Valutama": "777249250",
    "Prima Inti Valutama": "777248968",
    "Prima Sentral Dinamika": "777248574",
    "Pundimas Berkat Valasindo": "777249033",
    "Pundimas Galang Valasindo": "777248581",
    "Pusat Valas Indo": "777249480",
    "Raja Valutama Exchange": "777249078",
    "Rajasa Valasindo Utama": "777248872",
    "Rasgy Miola Valasindo": "777249015",
    "Rasilindo Usahatama Sukses": "777248961",
    "Ratu Aqila Universal": "777249043",
    "Ratu Dolarindo": "777249009",
    "Ratumas Valasindo": "777248608",
    "Restu indah Nusa Anugrah": "777248963",
    "Ricci Valas Indo": "777249242",
    "Royal Inti Valasindo": "777249282",
    "Sadila Persada Valas": "777248627",
    "Sagang Utama Jaya": "777248629",
    "Sahabat Valas": "777248633",
    "San Star Valutama": "777249144",
    "Santri Diwi": "777248640",
    "Saranatama Usaha Mandiri": "777248643",
    "Sari Arthagriha Valasindo Ekatama": "777249056",
    "Sejahtera Valasindo Abadi": "777248657",
    "Senopati Artha Utama": "777248665",
    "Sentral Utama Valas": "777249401",
    "Sentravalas Jaya Abadi": "777248946",
    "Simas Money Changer": "777953214",
    "Sinar Artha Fajar Ekatama": "777248680",
    "Sinar Jaya Valas": "777249238",
    "Sinar Mulia Jogja Valas": "777248692",
    "Sinar Utama Valasindo": "777249451",
    "Sip Valasindo": "777248984",
    "Sirampung Multi Valas": "777249454",
    "Solusi Mega Artha": "777248219",
    "Solusi Utama Valasindo": "777249426",
    "Sri Partha Utama Valasindo": "777248940",
    "Starindo Sugiarta": "777248706",
    "Sukses Inti Permata": "777249472",
    "Sukses Valasindo Sehati": "777249115",
    "Sulinggar Wirasta": "777248713",
    "Sumber Danatama Valasindo": "777248717",
    "Sumitan Artha Valutama": "777248718",
    "Supra Gading Raya": "777248723",
    "Surya Cahaya Valasindo": "777248885",
    "Surya Intervalas": "777248731",
    "Surya Utama Valasindo": "777249425",
    "Talent Valasindo": "777248737",
    "Telaga Harta Valasindo": "777249481",
    "Teratai Valasindo": "777248748",
    "Tetra Dua Sisi": "777953294",
    "Tisaga Berkah Valasindo": "777249158",
    "Tomiko Valas": "777248764",
    "Tompaschindo Valutama": "777248765",
    "Top Multi Valasindo": "777249149",
    "Tri Tunggal Berkah Mandiri": "777248953",
    "Tri Tunggal Devalas": "777248770",
    "Triguna Valasindo": "777248775",
    "Tunas Abadi Jaya Valasindo": "777248787",
    "Univalas Sejahtera": "777248795",
    "Univalas Sukses Sejahtera": "777249140",
    "Usaha Berkarya Sejahtera": "777249117",
    "Vacation Planners Valuta": "777248800",
    "Valas Inti Tolindo": "777953295",
    "Valuta Artha Mas": "777248810",
    "Valuta Cakra 21": "777248811",
    "Valuta Inti Berkatama": "777248814",
    "Valuta Inti Prima": "777953297",
    "Valutamart Visi Ideal Perkasa": "777249079",
    "Victori Karunia Valasindo": "777249264",
    "Vicvaluta Inter Perdana": "777248818",
    "Wahana Jaya Makmur": "777249094",
    "Wahyu Agung Santosa": "777248825",
    "Wira Usaha Maju Valasindo": "777248840",
    "Zed Artha Makmur": "777249241",
    "Gloria Maju Cahaya": "777249516",
    "Niaga Abadi Gemilang Adikarya": "777249523",
    "Berkat Sukses Trinitas": "777249535",
    "Jakarta Jaya Anugrah": "777249550",
    "Mitra Dwijaya Prima": "777249547",
    "Jeremy Orion Yuwana Valasindo": "777249558",
    "Zen Rizqi Valuta": "777249551",
    "Mitra Aksan Genggam": "777249563",
    "Widjaya Handal Artha": "777249574",
    "Multi Sinar Cahaya Makmur": "777249570",
    "Permata Valas Utama": "777249581",
    "Andalan Super Prioritas": "777249577",
    "Sariah Jaya Valas": "777249605",
    "Maju Berkah Valasindo": "777249596",
    "Mitra Bersaudara Valasindo": "777249595",
    "Nandy Valas Perkasa": "777249604",
    "Tiara Megah Sentosa": "777249641",
    "Sahla Berkah Bersama": "777249615",
    "Giga Anne Exchanger": "777249619",
    "Kantor Omzet Indonesia": "777249622",
    "Jakarta Arta Kencana": "777249620",
    "Raja Vallas Indonesia": "777249627",
    "Hai Hai Valasindo": "777249624",
    "Griya Valas Utama": "777249280",
    "Valuta Artha Mulia": "777249632",
    "Aneka Jaya Gemilang": "777249639",
    "Lima Triniti Jakarta Valasindo": "777249642",
    "Dewa Dolar Valasindo": "777249640",
    "Star Valas Indonesia": "777249650",
    "Dua Raja Valas": "777249654",
    "Daffa Indo Valuta": "777249656",
    "Lancar Sentosa Valasindo": "777249657",
    "MJC Money Changer": "777249658",
    "Dharma Artha Sejahtera": "777249660",
    "Gemar Arta Valutama": "777249659",
    "Dewata Inter Valasindo": "777249665",
    "Luxury Valuta Perkasa": "777249666",
    "Citra Abdi Valasindo": "777249664",
    "ZND Valuta Asia": "777249681",
    "Valuta Sukses Mandiri": "777249667",
    "Abadi Makmur Valasindo": "777249680",
    "Media Artha Valasindo": "777249687",
    "Bali Andatu Suksma": "777249689",
    "Ludu Artha Valas": "777249690",
    "Yakin Ekonomi Sukses": "777249685",
    "Pancaran Berkat Valasindo": "777249692",
    "Cahaya Ilona Persada": "777248147",
    "Dharta Aljazeera Global": "777249476",
    "Tom Valasindo": "777248763",
    "Arto Mili Makmur Abadi": "777249708",
    "Leo Jaya Insani Esa": "777249705",
    "Baiili Money Changer": "777249706",
    "Berkat Indovalas Gemilang": "777249707",
    "Varia Valuta Inti Prima": "777248938",
    "Panen Valas Abadi": "777249713",
    "Mulia Valuta Pratama": "777249715",
    "Karunia Aldana Valasindo": "777249716",
    "Bintang Omega Surya Semesta": "777249717",
    "Svarga Cakra Dana": "777249721",
    "Sriya Pundi Lestari": "777249722",
    "Lentera Pancar Dana": "777249723",
    "Mulia Emas Valasindo": "777249720",
    "Gemilang Sukses Valasindo": "777248975",
    "Prima Sukses Valasindo": "777249742",
    "Vic Vista Valutindo": "777249758",
    "Lintas Global Valas": "777249757",
    "Dinamis Sakti Kharisma": "777249756",
    "WLS Valas Indo": "777249759",
    "Daffa Valasindo Bhineka": "777249773",
    "Sembrani Pradipta Jaya": "777249768",
    "Harmoni Andalan Indovalas": "777249765",
    "Artha Modern Centre": "777248064",
    "Galuh Laras Utama": "777249785",
    "Rajhi Valuta Asing": "777249784",
    "Panda Valas Indonesia": "777249786",
    "Mei Pison Valasindo": "777249795",
    "Gerbang Indah Valuta Exchange": "777249794",
    "Mandala Valuta Asing Indonesia": "777249804",
    "Mentari Adhi Karya Sentosa": "777249805",
    "Globe Valas Sukses": "777249806",
    "Madalle Valuta Mandiri": "777249807",
    "Lima Benua Valasindo": "777249808",
    "Ziva Berkah Valasindo": "777249809",
}

# --- DATA PJP (Format Asli: Sandi NamaPT) ---
RAW_DATA_PJP = {
    "777930075": "KSP Indosurya Cipta",
    "777930030": "KSP Sahabat Mitra Sejati",
    "777958112": "PT Able Remittance",
    "777930105": "PT Achilles Financial Systems",
    "777958122": "PT Adisena Mitra Usaha",
    "777958111": "PT Agung Remittance Global",
    "777248909": "PT Andalusia Antar Benua",
    "777958119": "PT Artha Semesta Utama",
    "777958129": "PT Aryadana",
    "777930081": "PT Asia Fintek Teknologi",
    "777930077": "PT Asia Pelangi Remiten",
    "777930157": "PT Baiili Remitance Indonesia",
    "777959297": "PT Berkah Sinar Abadi",
    "777958121": "PT Berkat Gerizim",
    "777930152": "PT Cepat Indonesia Berkarya",
    "777958117": "PT Dhasatra Moneytransfer",
    "777930064": "PT Dompet Harapan Bangsa",
    "777930089": "PT Dhanapatra Loka Abadi",
    "777960626": "PT Duit Sono Sini Remittance",
    "777958115": "PT Eka Bakti Amerta Yoga Sejahtera",
    "777930026": "PT EMQ Indonesia Asia",
    "777930100": "PT Giat Bangun Indonesia",
    "777930015": "PT Fliptech Lentera Inspirasi Pertiwi",
    "777958132": "PT GPL Remittance Indonesia",
    "777248911": "PT Immer",
    "777958131": "PT Indo Koala Remittance",
    "777930079": "PT Indogo Express Remittance",
    "777958116": "PT Indomarco Prismatama",
    "777930120": "PT Infra Digital Nusantara",
    "777930094": "PT Inti Pratama Internasional",
    "777958675": "PT Intrajasa Teknosolusi",
    "777959090": "PT Jasa Kembar Kilat",
    "777958123": "PT Kangaroo Ausindo",
    "777930047": "PT Kawan Hai Hai Remittance",
    "777930131": "PT Kiriman Dana Pandai",
    "777930050": "PT Kredigram Pembayaran Elektronis",
    "777959414": "PT Lumbung Berkah Abadi",
    "777930018": "PT Madame Express Remitindo",
    "777958138": "PT Mana Payment Indonesia",
    "777930045": "PT Media Indonusa",
    "777930051": "PT Midi Utama Indonesia, Tbk",
    "777930044": "PT MDAQ Indonesia Technologies",
    "777958125": "PT MM Indonesia",
    "777957798": "PT Mobile Coin Asia",
    "777930097": "PT Moneta Pembayaran Teknologi",
    "777958799": "PT Moneygram Payment Systems Indonesia",
    "777930107": "PT Nandy Dana Express",
    "777930028": "PT Nusa Ekspresstama Remmitance",
    "777930065": "PT Nium Mitra Indonesia",
    "777111112": "PT Omnipay Transfer Dana (d/h Hayyu Indo)",
    "777958114": "PT Pacto",
    "777930104": "PT Paramas Murni Jaya",
    "777962666": "PT Payquest Network Indonesia",
    "777952522": "PT Pegadaian",
    "777930052": "PT Pelangi Indodata",
    "777930078": "PT Pelita Transfer Nusantara",
    "777959364": "PT Peniti Money Remittance",
    "777958127": "PT Prima Express Remit",
    "777958118": "PT Priority Valasindo Remittance",
    "777930108": "PT Pundimas Sentral Dana",
    "777930092": "PT Remit Express Indonesia",
    "777930031": "PT Remitindo Dolarasia Sejahtera",
    "777930103": "PT Silot Technology Indonesia",
    "777951823": "PT Sumber Alfaria Trijaya",
    "777958796": "PT Solusi Multi Artha",
    "777930076": "PT Sunrate Commercial Services",
    "777958110": "PT Syaftraco",
    "777958113": "PT Tiki Jalur Nugraha Ekakurir",
    "777930025": "PT Teleanjar Indonesia",
    "777111113": "PT Tranglo Indonesia",
    "777930127": "PT Tolong Transfer Paman Ryan",
    "777958130": "PT Uang Kita",
    "777953739": "PT VIP Remittance",
    "777930016": "PT VIT Remittance",
    "777958798": "PT Western Union Indonesia",
    "777958124": "PT Wintrust Indonesia",
    "777930096": "PT Wise Payments Indonesia",
    "777930069": "PT Zen Etransfer Nusantara",
    "777930048": "PT ZND Asia Utama",
}

# Mapping untuk normalisasi nama PJP
DATA_PJP_CLEAN = {}
for sandi, nama_raw in RAW_DATA_PJP.items():
    # Bersihkan PT dari nama di master data
    nama_clean = str(nama_raw).strip()
    if _PT_PREFIX_RE.match(nama_clean):
        nama_clean = _PT_PREFIX_RE.sub("", nama_clean, count=1).strip()
    DATA_PJP_CLEAN[nama_clean] = sandi

# Gabungkan semua data untuk lookup sandi
SANDI_MAPPING = DATA_KUPVA.copy()
SANDI_MAPPING.update(DATA_PJP_CLEAN)

# Mapping untuk Jenis Penyelenggara
JENIS_MAPPING = {}
for nama in DATA_KUPVA.keys():
    JENIS_MAPPING[nama] = "KUPVA BB"
for nama in DATA_PJP_CLEAN.keys():
    JENIS_MAPPING[nama] = "PJP (Transfer Dana)"

NAME_TRANSLATION = {
    # Format: normalized input -> output
    "PT Wira Maju Kencana": "Wira Maju Kencana",
    "PT Mekarindo Abadi Sentosa": "Bhaktishakti Ganda Intiharmoni",
    "PT Platinum Exchange (D/H Halim Inti Valasindo)": "Platinum Exchange",
    "PT Simas Money Changer / PT Shinta Forex": "Simas Money Changer",
    "PT Solusi Mega Artha (d/h PT Dinamis Citra Swakarsa)": "Solusi Mega Artha",
    "PT Valuta Artha Mas": "Valuta Artha Mas",
    "PT First Money (PT Mana Payment Indonesia)": "Mana Payment Indonesia",
    "PT First Money": "Mana Payment Indonesia",
    "PT First MoneyPT nandy dana ekspresPT Syaftraco (d/h CV. Syaftraco)": "Syaftraco",
    "PT Pnacaran Berkat Valasindo": "Pancaran Berkat Valasindo",
    "PT Eka Bakti Amerta Yoga Sejahtera (EBAYS)": "Eka Bakti Amerta Yoga Sejahtera",
    "PT VIT REMITTANCE INDONESIA": "VIT Remittance",
    "PT Natrabu Valas Permata": "Natrabu Valas Permata",
    "PT Wallex Teknologi Berkat": "MDAQ Indonesia Technologies",
}

# Membuat dict dictionary translation dalam huruf kecil (casefold) agar pengecekan case-insensitive
NAME_TRANSLATION_LOWER = {k.casefold(): v for k, v in NAME_TRANSLATION.items()}

# --- HELPER FUNCTIONS ---

def sandi_lookup(val):
    if pd.isna(val):
        return ""
    key = str(val).strip()
    return SANDI_MAPPING.get(key, "")

def jenis_lookup(val):
    if pd.isna(val):
        return ""
    key = str(val).strip()
    return JENIS_MAPPING.get(key, "")

def normalize_pt_name(value: object, *, force_prefix_pt: bool = True) -> object:
    """Normalize company names so PT prefix is consistent."""
    if pd.isna(value):
        return value
    
    text = str(value).strip()
    text = _WHITESPACE_RE.sub(" ", text)

    contains_koperasi = _KOPERASI_RE.search(text) is not None

    # Jika nama dimulai dengan 'SKP' (case-insensitive), jangan tambahkan 'PT'.
    # Jika ada prefix PT sebelum SKP, hapus prefix PT.
    if re.match(r'(?i)^\s*(pt\.?\s*)?skp\b', text):
        if _PT_PREFIX_RE.match(text):
            text = _PT_PREFIX_RE.sub("", text, count=1).strip()
        return text

    if _PT_PREFIX_RE.match(text):
        text = _PT_PREFIX_RE.sub("PT ", text, count=1)
        text = text.strip()
        if text.upper() == "PT":
            return "PT"
        return "PT " + text[2:].lstrip()

    if force_prefix_pt and not contains_koperasi:
        return f"PT {text}".strip()

    return text

def normalize_key_for_map(value: object) -> str:
    """Membersihkan nama dari PT untuk lookup ke dictionary."""
    if pd.isna(value):
        return ""
    text = str(value).strip()
    
    # Bersihkan keterangan (Persero) dan (d/h ...) agar tidak merusak skor fuzzy
    text = re.sub(r'(?i)\s*\((persero|d/h).*?\)', '', text)
    
    if _PT_PREFIX_RE.match(text):
        text = _PT_PREFIX_RE.sub("", text, count=1).strip()
        
    # Standardisasi typo/variasi ejaan umum secara otomatis
    text = re.sub(r'(?i)\bekspres\b', 'express', text)
    text = re.sub(r'(?i)\bremitance\b', 'remittance', text)
    text = re.sub(r'(?i)\bremintance\b', 'remittance', text)
    text = re.sub(r'(?i)\bremmitance\b', 'remittance', text)
    text = re.sub(r'(?i)\bvallas\b', 'valas', text)
    
    return text.strip()

def normalize_key(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    text = _WHITESPACE_RE.sub(" ", text)
    return text.casefold()

def parse_col_reference(col_ref: str) -> int | str:
    try:
        return int(col_ref)
    except ValueError:
        return col_ref

def pick_input_file(explicit: str | None) -> Path:
    if explicit:
        path = Path(explicit)
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")
        return path
    candidates = sorted(Path.cwd().glob("*.xlsx"))
    if not candidates:
        raise FileNotFoundError(
            "No .xlsx found in current folder. Provide --input <file.xlsx>."
        )
    return candidates[0]

def pick_non_locked_path(path: Path) -> Path:
    if not path.exists():
        return path
    for i in range(2, 51):
        candidate = path.with_name(f"{path.stem}__v{i}{path.suffix}")
        if not candidate.exists():
            return candidate
    return path

def excel_col_to_index(col: str) -> int:
    """Convert Excel column label (e.g. 'A', 'AG') to zero-based index."""
    label = str(col).strip().upper()
    if not label or not label.isalpha():
        raise ValueError(f"Invalid Excel column label: {col}")
    value = 0
    for ch in label:
        value = value * 26 + (ord(ch) - ord("A") + 1)
    return value - 1

# Daftar nama yang harus dikecualikan dari pencocokan (jangan beri skor tinggi)
EXCLUDE_RAW_NAMES = [
    "PT Intan Valas Abadi",
    "PT Surya Pertama Valasindo",
]

# Normalisasi kunci pengecualian akan dibuat saat runtime menggunakan
# fungsi normalize_key_for_map sehingga perbandingan konsisten.
EXCLUDE_KEYS = {normalize_key_for_map(n).casefold() for n in EXCLUDE_RAW_NAMES}

# Pengecualian khusus false-positive fuzzy (input -> kandidat match yang harus ditolak)
BLOCKED_FUZZY_PAIRS_RAW = [
    ("PT Lima Triniti Valasindo", "Lima Triniti Jakarta Valasindo"),
    ("PT Arta Raya Valasindo", "Arta Valasindo"),
    ("PT Mekar Bali Setia Abadi", "Mekar Setia Abadi"),
    ("PT Valasindo Dunia", "Dunia Multi Valasindo"),
    ("PT Java Arta Valasindo", "Arta Valasindo"),
    ("PT Shanaz Permata Valasindo", "Permata Valasindo"),
    ("PT Citra Valasindo Mulia", "Citra Valasindo"),
    ("PT Sahabat Citra Valas", "Sahabat Valas"),
    ("PT Jaya Utama Valasindo", "Karya Utama Valasindo"),
    ("PT Surya Batam Valasindo", "Surya Utama Valasindo"),
]

BLOCKED_FUZZY_PAIRS = {
    (normalize_key_for_map(src).casefold(), normalize_key_for_map(dst).casefold())
    for src, dst in BLOCKED_FUZZY_PAIRS_RAW
}

def fuzzy_match_name(name: object, *, score_cutoff: float = 70.0) -> tuple[str, float, str, str]:
    """Fuzzy match satu nama penyelenggara ke master mapping."""
    if pd.isna(name) or str(name).strip() == "":
        return ("", 0.0, "", "")

    query_raw = str(name).strip()
    raw_parts = [p.strip() for p in re.split(r"[/|]", query_raw) if p and p.strip()]
    if not raw_parts:
        raw_parts = [query_raw]

    # Matching dilakukan pada nama TANPA prefix PT (sudah ditangani normalize_key_for_map)
    query_parts = []
    for part in raw_parts:
        norm = normalize_key_for_map(part)
        norm = re.sub(r"[^0-9a-zA-Z\s]", " ", norm)
        norm = _WHITESPACE_RE.sub(" ", norm).strip()
        if norm:
            query_parts.append(norm)

    if not query_parts:
        return ("", 0.0, "", "")

    def _is_low_information(text: str) -> bool:
        tokens = text.split()
        if not tokens:
            return True
        joined_len = len("".join(tokens))
        if joined_len <= 2:
            return True
        if len(tokens) == 1 and len(tokens[0]) <= 2:
            return True
        return False

    # Contoh: "PT s", "PT a" -> jangan dipaksa fuzzy match
    if all(_is_low_information(p) for p in query_parts):
        return ("", 0.0, "", "")

    candidate_names = list(SANDI_MAPPING.keys())
    candidate_norms = [normalize_key_for_map(c) for c in candidate_names]
    candidate_norms_cf = [c.casefold() for c in candidate_norms]

    # Exact match (case-insensitive) per part
    for part in query_parts:
        part_cf = part.casefold()
        if part_cf in EXCLUDE_KEYS:
            return ("", 0.0, "", "")
        for i, cand_cf in enumerate(candidate_norms_cf):
            if cand_cf == part_cf:
                cand_name = candidate_names[i]
                return (cand_name, 100.0, SANDI_MAPPING.get(cand_name, ""), JENIS_MAPPING.get(cand_name, ""))

    best_cand = ""
    best_score = 0.0

    # Cari best score dari seluruh part (untuk menangani format "A / B")
    for part in query_parts:
        if _is_low_information(part):
            continue
        if _HAS_RAPIDFUZZ:
            result = process.extractOne(part, candidate_norms, scorer=fuzz.WRatio)
            if result:
                _matched_norm, score, idx = result
                if idx is not None and 0 <= idx < len(candidate_names):
                    if float(score) > best_score:
                        best_cand = candidate_names[idx]
                        best_score = float(score)
        else:
            for i, cand_norm in enumerate(candidate_norms):
                score = SequenceMatcher(None, part.casefold(), cand_norm.casefold()).ratio() * 100.0
                if score > best_score:
                    best_score = score
                    best_cand = candidate_names[i]

    # Tolak pasangan false-positive yang sudah ditandai khusus
    if best_cand:
        best_cand_norm = normalize_key_for_map(best_cand).casefold()
        query_cf_all = [p.casefold() for p in query_parts]
        if any((qcf, best_cand_norm) in BLOCKED_FUZZY_PAIRS for qcf in query_cf_all):
            return ("", min(round(best_score, 1), 89.0), "", "")

    if best_score < score_cutoff or not best_cand:
        return ("", round(best_score, 1), "", "")

    return (
        best_cand,
        round(best_score, 1),
        SANDI_MAPPING.get(best_cand, ""),
        JENIS_MAPPING.get(best_cand, ""),
    )

def process_rencana_edukasi(df: pd.DataFrame, *, force_prefix_pt: bool = True) -> pd.DataFrame:
    """
    Proses khusus Rencana Edukasi Konsumen:
        - Hapus kolom D, E, K, dan AG-CH
        - Merge nilai K + AG-CH menjadi satu kolom nama penyelenggara untuk matching
        - Kolom lain dibiarkan tetap
    - Fuzzy match nama penyelenggara ke master sandi/jenis
    """
    idx_d = excel_col_to_index("D")
    idx_e = excel_col_to_index("E")
    idx_k = excel_col_to_index("K")
    idx_ag = excel_col_to_index("AG")
    idx_ch = excel_col_to_index("CH")

    if len(df.columns) <= idx_ch:
        raise RuntimeError(
            "Jumlah kolom tidak mencukupi untuk membaca rentang AG-CH. "
            "Pastikan sheet yang dipilih adalah sheet data (contoh: 'Form1')."
        )

    col_k = df.columns[idx_k]

    ag_ch_indexes = list(range(idx_ag, idx_ch + 1))
    ag_ch_pairs = [(i, df.columns[i]) for i in ag_ch_indexes if i < len(df.columns)]

    rows: list[dict] = []

    for _, row in df.iterrows():
        # Siapkan baseline: semua kolom asli dipertahankan,
        # kecuali D, E, K, dan AG-CH sesuai permintaan.
        base = row.to_dict()

        # drop D, E, K, AG-CH
        if idx_d < len(df.columns):
            base.pop(df.columns[idx_d], None)
        if idx_e < len(df.columns):
            base.pop(df.columns[idx_e], None)
        if idx_k < len(df.columns):
            base.pop(df.columns[idx_k], None)
        for i in range(idx_ag, idx_ch + 1):
            if i < len(df.columns):
                base.pop(df.columns[i], None)

        nama_sumber: list[tuple[str, str]] = []

        # 1) Ambil dari kolom K
        val_k = row[col_k] if col_k in row.index else ""
        if pd.notna(val_k) and str(val_k).strip() != "":
            nama_sumber.append(("K", str(val_k).strip()))

        # 2) Ambil dari AG-CH lalu merge jadi satu kolom (unpivot vertikal)
        for idx, col_name in ag_ch_pairs:
            val = row[col_name]
            if pd.isna(val) or str(val).strip() == "":
                continue

            col_letter = ""
            n = idx + 1
            while n > 0:
                n, rem = divmod(n - 1, 26)
                col_letter = chr(rem + ord("A")) + col_letter

            nama_sumber.append((col_letter, str(val).strip()))

        for sumber_kolom, nama_raw in nama_sumber:
            row_out = dict(base)
            row_out["Sumber Kolom Nama"] = sumber_kolom
            row_out["Nama Penyelenggara Merge Raw"] = nama_raw
            rows.append(row_out)

    if not rows:
        empty_cols = [
            c for i, c in enumerate(df.columns)
            if i not in [idx_d, idx_e, idx_k] and not (idx_ag <= i <= idx_ch)
        ]
        empty_cols += [
            "Sumber Kolom Nama",
            "Nama Penyelenggara Merge Raw",
            "Nama Penyelenggara Merge",
            "Matched Name",
            "Match Perc",
            "Sandi",
            "Jenis Penyelenggara",
        ]
        return pd.DataFrame(columns=empty_cols)

    out = pd.DataFrame(rows)
    out["Nama Penyelenggara Merge"] = out["Nama Penyelenggara Merge Raw"].apply(
        lambda v: normalize_pt_name(v, force_prefix_pt=force_prefix_pt)
    )

    # Apply translation konsisten dengan pipeline nomor 1 (case-insensitive)
    def _translate(val: object) -> object:
        if pd.isna(val):
            return val
        key = str(val).strip().casefold()
        return NAME_TRANSLATION_LOWER.get(key, val)

    out["Nama Penyelenggara Merge"] = out["Nama Penyelenggara Merge"].apply(_translate)

    matched = out["Nama Penyelenggara Merge"].apply(fuzzy_match_name)
    matched_df = pd.DataFrame(matched.tolist(), columns=["Matched Name", "Match Perc", "Sandi", "Jenis Penyelenggara"])
    out = pd.concat([out.reset_index(drop=True), matched_df.reset_index(drop=True)], axis=1)

    # --- Seleksi final baris output ---
    # Tampilkan hanya:
    # 1) Match Perc >= 90, ATAU
    # 2) Satuan Kerja Bank Indonesia = KPwBI DKI Jakarta
    #    (jika score < 90 tetap ditampilkan, tapi Sandi dikosongkan)
    score_series = pd.to_numeric(out.get("Match Perc", 0), errors="coerce").fillna(0)

    satker_col = None
    if "Satuan Kerja Bank Indonesia" in out.columns:
        satker_col = "Satuan Kerja Bank Indonesia"
    else:
        for c in out.columns:
            if "satuan kerja bank indonesia" in str(c).casefold():
                satker_col = c
                break

    if satker_col:
        satker_series = out[satker_col].astype(str).str.casefold()
        is_dki = satker_series.str.contains("dki jakarta", na=False)
    else:
        is_dki = pd.Series(False, index=out.index)

    keep_mask = score_series.ge(90) | is_dki
    out = out.loc[keep_mask].copy()

    # DKI dengan score < 90: tetap tampil, tetapi tanpa sandi
    dki_low_mask = is_dki.loc[out.index] & score_series.loc[out.index].lt(90)
    if "Sandi" in out.columns:
        out.loc[dki_low_mask, "Sandi"] = ""

    # Untuk DKI dengan score < 90, pakai nama asli sebelum matching
    # (bukan nama kandidat hasil fuzzy)
    out["Nama Penyelenggara"] = out.get("Matched Name", "")
    if "Nama Penyelenggara Merge" in out.columns:
        out.loc[dki_low_mask, "Nama Penyelenggara"] = out.loc[dki_low_mask, "Nama Penyelenggara Merge"]

    # Format nama tampilan: tambahkan prefix PT,
    # kecuali jika diawali Koperasi atau KSP
    def _format_display_name(v: object) -> object:
        if pd.isna(v) or str(v).strip() == "":
            return v
        text = _WHITESPACE_RE.sub(" ", str(v).strip())
        if re.match(r"(?i)^\s*(koperasi|ksp)\b", text):
            return text
        return normalize_pt_name(text, force_prefix_pt=True)

    out["Nama Penyelenggara"] = out["Nama Penyelenggara"].apply(_format_display_name)

    # Rapikan duplikasi nama kolom "Jenis Penyelenggara"
    jp_positions = [i for i, c in enumerate(list(out.columns)) if c == "Jenis Penyelenggara"]
    if len(jp_positions) >= 2:
        cols = list(out.columns)
        cols[jp_positions[0]] = "Jenis Penyelenggara Form"
        cols[jp_positions[1]] = "Jenis Penyelenggara Match"
        out.columns = cols

    # --- Final seleksi kolom output (sesuai arahan user) ---
    # Hapus kolom yang mengandung kata kota/kabupaten pada header
    cols_drop_keyword = [
        c for c in out.columns
        if ("kota" in str(c).casefold()) or ("kabupaten" in str(c).casefold())
    ]

    # Hapus kolom spesifik: Wilayah (T), Sumber Kolom Nama (AD),
    # Nama Penyelenggara Merge Raw (AE), Nama Penyelenggara Merge (AF), Match Perc (AH)
    cols_drop_specific = [
        "Wilayah",
        "Sumber Kolom Nama",
        "Nama Penyelenggara Merge Raw",
        "Nama Penyelenggara Merge",
        "Match Perc",
        "Matched Name",
    ]

    out = out.drop(columns=cols_drop_keyword + cols_drop_specific, errors="ignore")

    # Taruh kolom hasil matching di awal
    leading_cols = [
        c for c in ["Nama Penyelenggara", "Sandi", "Jenis Penyelenggara Match", "Jenis Penyelenggara Form"]
        if c in out.columns
    ]
    remain_cols = [c for c in out.columns if c not in leading_cols]
    out = out[leading_cols + remain_cols]

    return out

def _header_key(text: object) -> str:
    s = str(text).casefold().strip()
    s = s.replace("�", " ")
    s = re.sub(r"\s+", " ", s)
    return s

def process_perlindungan_konsumen(
    df: pd.DataFrame,
    *,
    col_a: int | str = 0,
    col_b: int | str = 1,
    col_c: int | str = 2,
    force_prefix_pt: bool = True,
) -> pd.DataFrame:
    """
    Proses khusus Perlindungan Konsumen (mode default):
    - Normalisasi nama penyelenggara
    - Translate nama sesuai mapping
    - Fuzzy matching ke sandi/jenis
    - Filtering sesuai aturan existing script
    """

    out = df.copy()

    def _normalize_col_ref(ref: int | str) -> int | str:
        if isinstance(ref, int):
            return ref
        return parse_col_reference(str(ref))

    col_a_ref = _normalize_col_ref(col_a)
    col_b_ref = _normalize_col_ref(col_b)
    col_c_ref = _normalize_col_ref(col_c)

    # Resolve column references A and C first
    if isinstance(col_a_ref, int):
        if col_a_ref >= len(out.columns):
            raise RuntimeError(f"Index kolom nama tidak valid: {col_a_ref}")
        col_a_name = out.columns[col_a_ref]
    else:
        col_a_name = col_a_ref

    if isinstance(col_c_ref, int):
        col_c_name = out.columns[col_c_ref] if col_c_ref < len(out.columns) else None
    else:
        col_c_name = col_c_ref if col_c_ref in out.columns else None

    # Resolve column B (Completion time)
    col_b_name = None
    if isinstance(col_b_ref, int):
        if col_b_ref < len(out.columns):
            col_b_name = out.columns[col_b_ref]
    else:
        if col_b_ref in out.columns:
            col_b_name = col_b_ref

    # 1. RENAME COLUMN A to "Nama Penyelenggara"
    if col_a_name in out.columns:
        out.rename(columns={col_a_name: "Nama Penyelenggara"}, inplace=True)
        col_a_name = "Nama Penyelenggara"
    else:
        raise RuntimeError(f"Kolom nama penyelenggara tidak ditemukan: {col_a_name}")

    # 2. EXTRACT YEAR from Column B into "Tahun Laporan"
    if col_b_name and col_b_name in out.columns:
        dates = pd.to_datetime(out[col_b_name], errors='coerce')
        out["Tahun Laporan"] = dates.dt.year.astype('Int64')

    # 3. Normalize Names
    out[col_a_name] = out[col_a_name].apply(
        lambda v: normalize_pt_name(v, force_prefix_pt=force_prefix_pt)
    )

    # 4. Translate Names (case-insensitive)
    def translate_name(val):
        if pd.isna(val):
            return val
        key = str(val).strip().casefold()
        return NAME_TRANSLATION_LOWER.get(key, val)

    out[col_a_name] = out[col_a_name].apply(translate_name)

    # 5. Create 'Key' column for lookup
    out["_temp_lookup_key"] = out[col_a_name].apply(normalize_key_for_map)

    # 6. Skip KPwBI filtering — match on names only and keep all rows
    df_kept = out.copy()
    df_excluded = pd.DataFrame(columns=out.columns)

    # 7. Isi Kolom Sandi & Jenis dengan fuzzy matching (rapidfuzz preferred)
    candidate_names = list(SANDI_MAPPING.keys())

    def lookup_fuzzy(name: object, kpwbi_val: object):
        # returns (matched_name, match_pct, sandi, jenis)
        if pd.isna(name) or str(name).strip() == "":
            return ("", 0.0, "", "")

        key = str(name).strip()
        parts = [p.strip() for p in key.split('/') if p.strip()]

        best_cand = None
        best_score = 0.0

        # Cek apakah KPwBI-nya DKI Jakarta atau Kantor Pusat
        is_dki = False
        if pd.notna(kpwbi_val):
            kpw_str = str(kpwbi_val).lower()
            if "dki jakarta" in kpw_str or "kantor pusat" in kpw_str:
                is_dki = True

        for part in parts:
            part_lower = part.casefold()
            part_lower = re.sub(r'^pt\.?\s+', '', part_lower).strip()

            # Jika nama ini ada dalam daftar pengecualian, jangan beri skor tinggi
            try:
                norm_part_key = normalize_key_for_map(part).casefold()
            except Exception:
                norm_part_key = part_lower
            if norm_part_key in EXCLUDE_KEYS:
                return ("", 0.0, "", "")

            # Exact match check first (case-insensitive)
            for cand, sandi in SANDI_MAPPING.items():
                if cand.casefold() == part_lower:
                    return (cand, 100.0, sandi, JENIS_MAPPING.get(cand, ""))

            part_words = part_lower.split()

            # Iterate candidates and compute score
            for cand in candidate_names:
                cand_lower = cand.casefold()
                cand_words = cand_lower.split()

                if _HAS_RAPIDFUZZ:
                    score_ratio = fuzz.ratio(part_lower, cand_lower)
                    score_sort = fuzz.token_sort_ratio(part_lower, cand_lower)
                    score_set = fuzz.token_set_ratio(part_lower, cand_lower)
                    adjusted = max(score_ratio, score_sort)

                    word_diff = abs(len(part_words) - len(cand_words))
                    if score_set > 85 and word_diff > 0:
                        if is_dki:
                            penalty = 5 * word_diff
                        else:
                            penalty = 15 * word_diff
                        subset_score = score_set - penalty
                        adjusted = max(adjusted, subset_score)
                else:
                    adjusted = SequenceMatcher(None, part_lower, cand_lower).ratio() * 100.0
                    word_diff = abs(len(part_words) - len(cand_words))
                    if word_diff > 0:
                        adjusted -= (10 * word_diff) if not is_dki else (3 * word_diff)

                if adjusted > best_score:
                    best_score = adjusted
                    best_cand = cand

        if best_cand:
            sandi = SANDI_MAPPING.get(best_cand, "")
            jenis = JENIS_MAPPING.get(best_cand, "")
            return (best_cand, round(best_score, 1), sandi, jenis)

        return ("", 0.0, "", "")

    def apply_fuzzy_to_df(dframe: pd.DataFrame) -> pd.DataFrame:
        if dframe.empty:
            for c in ["Sandi", "Jenis Penyelenggara", "Matched Name", "Match Perc"]:
                if c not in dframe.columns:
                    dframe[c] = pd.Series(dtype=object)
            return dframe

        def fuzzy_wrapper(row):
            kpw_val = row[col_c_name] if col_c_name and (col_c_name in row.index) else ""
            return pd.Series(lookup_fuzzy(row["_temp_lookup_key"], kpw_val))

        fuzzy_series = dframe.apply(fuzzy_wrapper, axis=1)
        fuzzy_series.columns = ["Matched Name", "Match Perc", "Sandi", "Jenis Penyelenggara"]

        dframe = dframe.reset_index(drop=True).join(fuzzy_series.reset_index(drop=True))

        cols = [c for c in dframe.columns if c != "Match Perc"]
        if "Nama Penyelenggara" in cols:
            base_idx = cols.index("Nama Penyelenggara") + 1
            for c in ["Sandi", "Jenis Penyelenggara", "Matched Name"]:
                if c in cols:
                    cols.remove(c)
            for i, c in enumerate(["Sandi", "Jenis Penyelenggara", "Matched Name"]):
                cols.insert(base_idx + i, c)

        cols.append("Match Perc")
        cols = [c for c in cols if c in dframe.columns]
        dframe = dframe[cols]
        return dframe

    df_kept = apply_fuzzy_to_df(df_kept)
    if not df_excluded.empty:
        df_excluded = apply_fuzzy_to_df(df_excluded)

    # --- Post-processing ---
    if "Matched Name" in df_kept.columns:
        df_kept = df_kept.drop(columns=["Matched Name"])

    if col_c_name and col_c_name in df_kept.columns:
        df_kept = df_kept.drop(columns=[col_c_name])

    if "Match Perc" in df_kept.columns:
        df_kept["Match Perc"] = pd.to_numeric(df_kept["Match Perc"], errors='coerce').fillna(0)
        df_kept = df_kept[df_kept["Match Perc"] > 94].reset_index(drop=True)
        df_kept = df_kept.drop(columns=["Match Perc"])

    if not df_kept.empty:
        cols = list(df_kept.columns)
        if "Nama Penyelenggara" in cols:
            new_cols = ["Nama Penyelenggara"]
            if "Sandi" in cols:
                new_cols.append("Sandi")
            if "Tahun Laporan" in cols:
                new_cols.append("Tahun Laporan")
            if "Periode Laporan" in cols:
                new_cols.append("Periode Laporan")
            if "Jenis Penyelenggara" in cols:
                new_cols.append("Jenis Penyelenggara")
            for c in cols:
                if c not in new_cols:
                    new_cols.append(c)
            df_kept = df_kept[new_cols]

    if not df_excluded.empty:
        if "Match Perc" in df_excluded.columns:
            mask_promote = df_excluded["Match Perc"].ge(90)
            if mask_promote.any():
                to_promote = df_excluded.loc[mask_promote].copy()
                df_kept = pd.concat([df_kept, to_promote], ignore_index=True)

    df_kept = df_kept.drop(columns=["_temp_lookup_key"], errors='ignore')
    return df_kept

def process_realisasi_edukasi(df: pd.DataFrame, *, force_prefix_pt: bool = True) -> pd.DataFrame:
    """
    Proses khusus Realisasi Edukasi Publik:
    - Sumber matching: Cleaning_Nama Penyelenggara
    - Rename jadi Nama Penyelenggara
    - Hapus kolom-kolom yang diminta
    - Tampilkan score match untuk review
    """
    source_col = "Cleaning_Nama Penyelenggara"
    if source_col not in df.columns:
        raise RuntimeError("Kolom 'Cleaning_Nama Penyelenggara' tidak ditemukan.")

    out = df.copy()

    nama_tmp_col = "__nama_penyelenggara_for_match"
    nama_final_col = "__nama_penyelenggara_final"

    out[nama_tmp_col] = out[source_col].apply(
        lambda v: normalize_pt_name(v, force_prefix_pt=force_prefix_pt)
    )

    def _translate(val: object) -> object:
        if pd.isna(val):
            return val
        key = str(val).strip().casefold()
        return NAME_TRANSLATION_LOWER.get(key, val)

    out[nama_tmp_col] = out[nama_tmp_col].apply(_translate)

    matched = out[nama_tmp_col].apply(fuzzy_match_name)
    matched_df = pd.DataFrame(
        matched.tolist(),
        columns=["Matched Name", "Match Perc", "Sandi", "Jenis Penyelenggara Match"],
    )
    out = pd.concat([out.reset_index(drop=True), matched_df.reset_index(drop=True)], axis=1)

    # Filtering final seperti mode sebelumnya:
    # keep score >= 90, atau baris DKI Jakarta
    score_series = pd.to_numeric(out.get("Match Perc", 0), errors="coerce").fillna(0)
    satker_col = "Satuan Kerja Bank Indonesia" if "Satuan Kerja Bank Indonesia" in out.columns else None
    if satker_col:
        satker_series = out[satker_col].astype(str).str.casefold()
        is_dki = satker_series.str.contains("dki jakarta", na=False)
    else:
        is_dki = pd.Series(False, index=out.index)

    keep_mask = score_series.ge(90) | is_dki
    out = out.loc[keep_mask].copy()

    # DKI dengan score < 90 tetap ditampilkan, tapi tanpa Sandi
    dki_low_mask = is_dki.loc[out.index] & score_series.loc[out.index].lt(90)
    if "Sandi" in out.columns:
        out.loc[dki_low_mask, "Sandi"] = ""

    # Nama final:
    # - default dari hasil matching
    # - jika DKI score < 90 pakai nama asli sebelum matching
    out[nama_final_col] = out.get("Matched Name", "")
    out.loc[dki_low_mask, nama_final_col] = out.loc[dki_low_mask, nama_tmp_col]

    # Tambah prefix PT, kecuali diawali Koperasi/KSP
    def _format_display_name(v: object) -> object:
        if pd.isna(v) or str(v).strip() == "":
            return v
        text = _WHITESPACE_RE.sub(" ", str(v).strip())
        if re.match(r"(?i)^\s*(koperasi|ksp)\b", text):
            return text
        return normalize_pt_name(text, force_prefix_pt=True)

    out[nama_final_col] = out[nama_final_col].apply(_format_display_name)

    drop_names = {
        "total aset tahun ini (dalam rupiah)",
        "target aset tahun depan (dalam rupiah)",
        "pendapatan tahun lalu (dalam rupiah)",
        "pendapatan tahun ini (dalam rupiah)",
        "target pendapatan tahun depan (dalam rupiah)",
        "biaya tahun lalu (dalam rupiah)",
        "biaya tahun ini (dalam rupiah)",
        "target biaya tahun depan (dalam rupiah)",
        "jumlah konsumen tahun depan",
        "faktor yang memengaruhi operasional",
        "sasaran",
        "target jumlah peserta",
        "materi atau konten edukasi",
        "kanal edukasi",
        "media atau metode edukasi",
        "jumlah kegiatan per media dan/atau metode edukasi",
        "wilayah",
        "provinsi",
        "kota",
        "kabupaten",
        "email",
        "cleaning_completion cleaning",
        "nama penyelenggara",
        "cleaning_nama penyelenggara",
    }

    cols_to_drop = []
    for c in out.columns:
        ck = _header_key(c)
        ck = re.sub(r"\s+", " ", ck)
        if ck in drop_names:
            cols_to_drop.append(c)

    out = out.drop(columns=cols_to_drop, errors="ignore")

    # Buang kolom intermediate yang tidak perlu tampil
    out = out.drop(columns=["Matched Name", nama_tmp_col], errors="ignore")

    # Rename nama final ke header akhir yang diminta
    if nama_final_col in out.columns:
        out = out.rename(columns={nama_final_col: "Nama Penyelenggara"})

    # Match Perc dipakai untuk filtering saja, tidak ditampilkan di output akhir
    out = out.drop(columns=["Match Perc"], errors="ignore")

    front_cols = [
        c for c in [
            "Nama Penyelenggara",
            "Sandi",
            "Jenis Penyelenggara Match",
        ] if c in out.columns
    ]
    rest_cols = [c for c in out.columns if c not in front_cols]
    out = out[front_cols + rest_cols]

    return out

LTDBB_VALID_VARIANTS = {"G0001", "G0002", "G0003"}
LTDBB_COLUMN_PATTERNS = {
    "Frekuensi Pengiriman": [
        r"\bfrekuensi\b.*\bpengiriman\b",
        r"\bjumlah\b.*\bpengiriman\b",
        r"\btotal\b.*\bfrekuensi\b",
    ],
    "Total Nominal Transaksi": [
        r"\btotal\b.*\bnominal\b.*\btransaksi\b",
        r"\bnominal\b.*\btransaksi\b",
        r"\bjumlah\b.*\bnominal\b",
    ],
    "Negara Tujuan Pengiriman": [
        r"\bnegara\b.*\btujuan\b.*\bpengiriman\b",
        r"\btujuan\b.*\bnegara\b",
    ],
    "Kota/Kab. Tujuan Pengiriman": [
        r"\bkota\b.*\btujuan\b.*\bpengiriman\b",
        r"\bkab(?:upaten)?\b.*\btujuan\b.*\bpengiriman\b",
        r"\bkota\s*kab\b.*\btujuan\b.*\bpengiriman\b",
    ],
}


def _ltdbb_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).replace("\xa0", " ").replace("\n", " ").strip()
    text = _WHITESPACE_RE.sub(" ", text)
    return "" if text.casefold() in {"nan", "none"} else text


def _ltdbb_header_key(text: object) -> str:
    key = _ltdbb_text(text).casefold()
    key = re.sub(r"[^0-9a-z]+", " ", key)
    return _WHITESPACE_RE.sub(" ", key).strip()


def _ltdbb_unique_headers(headers: list[object]) -> list[str]:
    counts: dict[str, int] = {}
    unique_headers: list[str] = []
    for idx, header in enumerate(headers, start=1):
        base = _ltdbb_text(header) or f"Kolom {idx}"
        seen = counts.get(base, 0) + 1
        counts[base] = seen
        unique_headers.append(base if seen == 1 else f"{base} ({seen})")
    return unique_headers


def _ltdbb_parse_number(value: object) -> object:
    if pd.isna(value):
        return pd.NA

    text = _ltdbb_text(value)
    if text == "":
        return pd.NA

    text = re.sub(r"[^0-9,\.\-]", "", text)
    if text in {"", "-", ".", ",", "-,"}:
        return pd.NA

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        tail = text.rsplit(",", 1)[-1]
        if tail.isdigit() and len(tail) <= 2:
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif text.count(".") > 1:
        text = text.replace(".", "")

    return pd.to_numeric(text, errors="coerce")


def _ltdbb_extract_metadata(df_raw: pd.DataFrame) -> dict[str, object]:
    meta: dict[str, object] = {
        "pjp_name": None,
        "pjp_sandi": None,
        "periode_text": None,
    }

    candidate_lines: list[str] = []
    for row_idx, col_idx in [(1, 2), (2, 2)]:
        if row_idx < len(df_raw.index) and col_idx < df_raw.shape[1]:
            text = _ltdbb_text(df_raw.iat[row_idx, col_idx])
            if text:
                candidate_lines.append(text)

    for row_idx in range(min(10, len(df_raw.index))):
        row_texts = [_ltdbb_text(v) for v in df_raw.iloc[row_idx].tolist()]
        row_texts = [v for v in row_texts if v]
        if row_texts:
            candidate_lines.append(" | ".join(row_texts))

    def _clean_value(text: str) -> str:
        cleaned = re.sub(r"^[^:|-]*[:|-]\s*", "", text).strip()
        return cleaned or text.strip()

    for line in candidate_lines:
        lower_line = line.casefold()

        if meta["pjp_sandi"] is None:
            sandi_match = re.search(r"\bsandi(?:\s+pjp)?\b[^0-9]*(\d{3,})", line, flags=re.IGNORECASE)
            if sandi_match:
                meta["pjp_sandi"] = sandi_match.group(1)

        if meta["periode_text"] is None:
            periode_match = re.search(r"\bperiode\b\s*[:|-]?\s*(.+)", line, flags=re.IGNORECASE)
            if periode_match:
                meta["periode_text"] = periode_match.group(1).strip()
            elif re.search(r"\b(20\d{2}|januari|februari|maret|april|mei|juni|juli|agustus|september|oktober|november|desember|semester|triwulan|bulan)\b", lower_line):
                meta["periode_text"] = _clean_value(line)

        if meta["pjp_name"] is None:
            name_match = re.search(
                r"\b(?:nama\s*pjp|nama\s*penyelenggara|penyelenggara|pjp)\b\s*[:|-]\s*(.+)",
                line,
                flags=re.IGNORECASE,
            )
            if name_match:
                meta["pjp_name"] = name_match.group(1).strip()

    for line in candidate_lines:
        if meta["periode_text"] is None and re.search(r"\b(20\d{2}|januari|februari|maret|april|mei|juni|juli|agustus|september|oktober|november|desember)\b", line, flags=re.IGNORECASE):
            meta["periode_text"] = _clean_value(line)
        if meta["pjp_name"] is None and not re.search(r"\bperiode\b", line, flags=re.IGNORECASE):
            cleaned = _clean_value(line)
            if cleaned and not re.fullmatch(r"\d{3,}", cleaned):
                meta["pjp_name"] = cleaned
        if meta["pjp_sandi"] is not None and meta["pjp_name"] is not None and meta["periode_text"] is not None:
            break

    return meta


def _ltdbb_detect_variant(columns: list[str], *, filename: str = "", metadata_blob: str = "") -> str | None:
    text = f"{filename} {metadata_blob}".casefold()
    column_set = set(columns)

    if "Negara Tujuan Pengiriman" in column_set:
        return "G0001"
    if re.search(r"\bg0001\b|outgoing|luar\s+negeri", text):
        return "G0001"
    if re.search(r"\bg0002\b|ingoing|incoming", text):
        return "G0002"
    if re.search(r"\bg0003\b|domestik|domestic", text):
        return "G0003"
    if "Kota/Kab. Tujuan Pengiriman" in column_set:
        return "G0002"
    return None


def process_ltdbb_cleaner(
    df_raw: pd.DataFrame,
    *,
    filename: str = "",
    variant_override: str | None = None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """
    Bersihkan template LTDBB/LKPBU mentah:
    - gunakan baris ke-6 sebagai header
    - buang kolom A dan B
    - hapus banner/footer kosong atau total
    - normalisasi nama kolom penting otomatis
    - hitung metadata ringkas untuk dashboard
    """
    if variant_override and variant_override not in LTDBB_VALID_VARIANTS:
        raise ValueError("Variant override tidak valid. Gunakan G0001, G0002, atau G0003.")

    if df_raw.empty or len(df_raw.index) < 6:
        raise ValueError("Template LTDBB tidak valid. Minimal harus memiliki 6 baris.")

    metadata = _ltdbb_extract_metadata(df_raw)

    header_row = [_ltdbb_text(v) for v in df_raw.iloc[5].tolist()]
    data = df_raw.iloc[6:].copy().reset_index(drop=True)

    if data.shape[1] <= 2:
        raise ValueError("Kolom data LTDBB tidak ditemukan setelah kolom A dan B dibuang.")

    header_row = header_row[2:]
    data = data.iloc[:, 2:].copy()
    data.columns = _ltdbb_unique_headers(header_row)

    data = data.apply(lambda col: col.map(lambda value: pd.NA if _ltdbb_text(value) == "" else value))
    data = data.dropna(axis=1, how="all").dropna(axis=0, how="all").reset_index(drop=True)

    header_keys = [_ltdbb_header_key(col) for col in data.columns]
    keep_mask: list[bool] = []
    for _, row in data.iterrows():
        row_values = [_ltdbb_text(v) for v in row.tolist()]
        non_empty = [v for v in row_values if v]
        joined = " ".join(non_empty).casefold()
        row_keys = [_ltdbb_header_key(v) for v in non_empty]

        sample_size = min(3, len(header_keys), len(row_keys))
        is_repeated_header = sample_size >= 2 and row_keys[:sample_size] == header_keys[:sample_size]
        is_footer = bool(
            re.search(
                r"\b(grand total|jumlah total|catatan|keterangan|note|dicetak pada|halaman)\b",
                joined,
            )
        )
        keep_mask.append(bool(non_empty) and not is_repeated_header and not is_footer)

    data = data.loc[keep_mask].reset_index(drop=True)

    rename_map: dict[str, str] = {}
    used_targets: set[str] = set()
    for col in data.columns:
        key = _ltdbb_header_key(col)
        for canonical, patterns in LTDBB_COLUMN_PATTERNS.items():
            if canonical in used_targets:
                continue
            canonical_key = _ltdbb_header_key(canonical)
            if key == canonical_key or any(re.search(pattern, key) for pattern in patterns):
                rename_map[col] = canonical
                used_targets.add(canonical)
                break
    data = data.rename(columns=rename_map)

    for numeric_col in ["Frekuensi Pengiriman", "Total Nominal Transaksi"]:
        if numeric_col in data.columns:
            data[numeric_col] = data[numeric_col].map(_ltdbb_parse_number).astype("Float64")

    for text_col in ["Negara Tujuan Pengiriman", "Kota/Kab. Tujuan Pengiriman"]:
        if text_col in data.columns:
            data[text_col] = data[text_col].map(lambda value: pd.NA if _ltdbb_text(value) == "" else _ltdbb_text(value))
            mask_total = data[text_col].astype("string").str.contains(r"\btotal\b", case=False, na=False)
            data = data.loc[~mask_total].copy()

    important_cols = [
        col for col in [
            "Negara Tujuan Pengiriman",
            "Kota/Kab. Tujuan Pengiriman",
            "Frekuensi Pengiriman",
            "Total Nominal Transaksi",
        ] if col in data.columns
    ]
    if important_cols:
        data = data.loc[~data[important_cols].isna().all(axis=1)].copy()

    data = data.reset_index(drop=True)

    metadata_blob = " ".join(str(value) for value in metadata.values() if value)
    variant = variant_override or _ltdbb_detect_variant(list(data.columns), filename=filename, metadata_blob=metadata_blob)

    metadata.update({
        "variant": variant,
        "rows": int(len(data.index)),
        "total_frekuensi": float(data["Frekuensi Pengiriman"].fillna(0).sum()) if "Frekuensi Pengiriman" in data.columns else None,
        "total_nominal": float(data["Total Nominal Transaksi"].fillna(0).sum()) if "Total Nominal Transaksi" in data.columns else None,
    })

    return data, metadata

# --- MAIN LOGIC ---

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clean Excel: normalize PT, exclude rows, fill LKPBU, and extract Year."
    )

    parser.add_argument("--input", help="Path file Excel input.", default=None)
    parser.add_argument(
        "--mode",
        choices=["perlindungan-konsumen", "rencana-edukasi", "realisasi-edukasi-publik"],
        default="perlindungan-konsumen",
        help="Pilih mode pengolahan file.",
    )
    parser.add_argument("--sheet", help="Nama sheet atau index.", default=0)
    parser.add_argument("--col-a", help="Kolom nama PT.", default="0")
    parser.add_argument("--col-b", help="Kolom waktu (Completion time).", default="1")
    parser.add_argument("--col-c", help="Kolom filter KPwBI.", default="2")
    parser.add_argument(
        "--force-prefix-pt",
        help="Tambah prefix PT jika tidak ada.",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--output-clean", help="Path output clean.", default=None)
    parser.add_argument("--output-excluded", help="Path output excluded.", default=None)

    args = parser.parse_args()

    input_path = pick_input_file(args.input)
    stem = input_path.stem
    default_clean = input_path.with_name(f"{stem}__cleaned.xlsx")
    default_excluded = input_path.with_name(f"{stem}__excluded.xlsx")
    output_clean = Path(args.output_clean) if args.output_clean else default_clean
    output_excluded = (
        Path(args.output_excluded) if args.output_excluded else default_excluded
    )

    col_a_ref = parse_col_reference(args.col_a)
    col_b_ref = parse_col_reference(args.col_b)
    col_c_ref = parse_col_reference(args.col_c)

    try:
        df = pd.read_excel(input_path, sheet_name=args.sheet)
    except Exception as exc:
        raise RuntimeError("Gagal membaca Excel.") from exc

    # Auto-switch mode untuk file Rencana Pelaksanaan Edukasi jika user belum mengubah --mode
    selected_mode = args.mode
    if selected_mode == "perlindungan-konsumen":
        stem_cf = input_path.stem.casefold()
        if "rencana" in stem_cf and "edukasi" in stem_cf:
            selected_mode = "rencana-edukasi"
        elif "pelaksanaan edukasi" in stem_cf:
            selected_mode = "realisasi-edukasi-publik"

    # Pipeline khusus Rencana Edukasi Konsumen
    if selected_mode == "rencana-edukasi":
        # Jika sheet default (0) ternyata bukan sheet data, fallback ke 'Form1'
        if (args.sheet == 0 or args.sheet == "0") and len(df.columns) < excel_col_to_index("CH") + 1:
            try:
                df = pd.read_excel(input_path, sheet_name="Form1")
            except Exception:
                pass

        result_df = process_rencana_edukasi(df, force_prefix_pt=args.force_prefix_pt)

        default_db = input_path.with_name(f"{stem}__database_rencana_edukasi.xlsx")
        output_db = output_clean if args.output_clean else default_db

        try:
            with pd.ExcelWriter(output_db) as writer:
                result_df.to_excel(writer, index=False)
        except PermissionError:
            output_db = pick_non_locked_path(output_db)
            with pd.ExcelWriter(output_db) as writer:
                result_df.to_excel(writer, index=False)

        print("Selesai.")
        print(f"Mode           : {selected_mode}")
        print(f"Output database: {output_db}")
        print(f"Total baris DB : {len(result_df)}")
        return 0

    # Pipeline khusus Realisasi Edukasi Publik
    if selected_mode == "realisasi-edukasi-publik":
        result_df = process_realisasi_edukasi(df, force_prefix_pt=args.force_prefix_pt)

        default_db = input_path.with_name(f"{stem}__database_realisasi_edukasi.xlsx")
        output_db = output_clean if args.output_clean else default_db

        try:
            with pd.ExcelWriter(output_db) as writer:
                result_df.to_excel(writer, index=False)
        except PermissionError:
            output_db = pick_non_locked_path(output_db)
            with pd.ExcelWriter(output_db) as writer:
                result_df.to_excel(writer, index=False)

        print("Selesai.")
        print(f"Mode           : {selected_mode}")
        print(f"Output database: {output_db}")
        print(f"Total baris DB : {len(result_df)}")
        return 0

    # Resolve column references A and C first
    if isinstance(col_a_ref, int):
        col_a_name = df.columns[col_a_ref]
    else:
        col_a_name = col_a_ref

    if isinstance(col_c_ref, int):
        col_c_name = df.columns[col_c_ref]
    else:
        col_c_name = col_c_ref

    # Resolve column B (Completion time)
    col_b_name = None
    if isinstance(col_b_ref, int):
        if col_b_ref < len(df.columns):
            col_b_name = df.columns[col_b_ref]
    else:
        if col_b_ref in df.columns:
            col_b_name = col_b_ref

    # 1. RENAME COLUMN A to "Nama Penyelenggara" (as requested)
    if col_a_name in df.columns:
        df.rename(columns={col_a_name: "Nama Penyelenggara"}, inplace=True)
        col_a_name = "Nama Penyelenggara" # Update variable ref

    # 2. EXTRACT YEAR from Column B into "Tahun Laporan"
    if col_b_name and col_b_name in df.columns:
        try:
            # Convert to datetime, extract Year
            dates = pd.to_datetime(df[col_b_name], errors='coerce')
            df["Tahun Laporan"] = dates.dt.year.astype('Int64') # Int64 allows NaN
        except Exception as e:
            print(f"Warning: Gagal mengekstrak tahun dari kolom {col_b_name}. Error: {e}")

    # 3. Normalize Names (Adding PT prefix where missing)
    df[col_a_name] = df[col_a_name].apply(
        lambda v: normalize_pt_name(v, force_prefix_pt=args.force_prefix_pt)
    )

    # 4. Translate Names (Perbaikan: Case-Insensitive)
    def translate_name(val):
        if pd.isna(val):
            return val
        key = str(val).strip()
        key_lower = key.casefold() # Gunakan lowercase untuk mencari di mapping
        return NAME_TRANSLATION_LOWER.get(key_lower, val)
        
    df[col_a_name] = df[col_a_name].apply(translate_name)

    # 5. Create 'Key' column for lookup
    df["_temp_lookup_key"] = df[col_a_name].apply(normalize_key_for_map)

    # 6. Skip KPwBI filtering — match on names only and keep all rows
    df_kept = df.copy()
    df_excluded = pd.DataFrame(columns=df.columns)

    # 7. Isi Kolom Sandi & Jenis dengan fuzzy matching (rapidfuzz preferred)
    candidate_names = list(SANDI_MAPPING.keys())

    def lookup_fuzzy(name: object, kpwbi_val: object):
        # returns (matched_name, match_pct, sandi, jenis)
        if pd.isna(name) or str(name).strip() == "":
            return ("", 0.0, "", "")
            
        key = str(name).strip()
        
        # PENTING: Pecah berdasarkan garis miring '/' jika ada (biasanya dipakai untuk alias/merger)
        # Dengan ini, "Simas Money Changer / PT. Shinta Forex" akan dievaluasi secara terpisah!
        parts = [p.strip() for p in key.split('/') if p.strip()]

        best_cand = None
        best_score = 0.0

        # Cek apakah KPwBI-nya DKI Jakarta atau Kantor Pusat
        is_dki = False
        if pd.notna(kpwbi_val):
            kpw_str = str(kpwbi_val).lower()
            if "dki jakarta" in kpw_str or "kantor pusat" in kpw_str:
                is_dki = True

        for part in parts:
            part_lower = part.casefold()
            
            # Kita bersihkan juga "pt " atau "pt. " yang mungkin ada di awal setiap potongan
            part_lower = re.sub(r'^pt\.?\s+', '', part_lower).strip()

            # Jika nama ini ada dalam daftar pengecualian, jangan beri skor tinggi
            try:
                norm_part_key = normalize_key_for_map(part).casefold()
            except Exception:
                norm_part_key = part_lower
            if norm_part_key in EXCLUDE_KEYS:
                # Kembalikan tanpa sandi dan skor 0 sehingga tidak dipromosikan
                return ("", 0.0, "", "")

            # Exact match check first (case-insensitive) untuk potongan kata ini
            for cand, sandi in SANDI_MAPPING.items():
                if cand.casefold() == part_lower:
                    return (cand, 100.0, sandi, JENIS_MAPPING.get(cand, ""))
            
            part_words = part_lower.split()
            
            # Iterate candidates and compute score
            for cand in candidate_names:
                cand_lower = cand.casefold()
                cand_words = cand_lower.split()

                if _HAS_RAPIDFUZZ:
                    # Perbaikan Logika Scoring Canggih:
                    score_ratio = fuzz.ratio(part_lower, cand_lower)
                    score_sort = fuzz.token_sort_ratio(part_lower, cand_lower)
                    score_set = fuzz.token_set_ratio(part_lower, cand_lower)
                    
                    # Dasar perhitungan: Ambil skor karakter (untuk akomodasi typo) atau sort (urutan acak)
                    adjusted = max(score_ratio, score_sort)
                    
                    # Cek jika ada perbedaan jumlah kata (e.g., 3 kata vs 2 kata)
                    word_diff = abs(len(part_words) - len(cand_words))
                    
                    # Jika token set rasio sangat tinggi (artinya salah satu kata adalah singkatan/bagian dari yang lain)
                    # DAN ada perbedaan jumlah kata (untuk menghindari "Arta Valasindo" 100% sama dengan "Java Arta Valasindo")
                    if score_set > 85 and word_diff > 0:
                        if is_dki:
                            # Jika dia dari DKI / Kantor Pusat, kita beri toleransi ringan (misal hanya beda 1 kata -> potong 5 poin)
                            penalty = 5 * word_diff
                        else:
                            # Jika BUKAN DKI, kita hajar penalti berat agar skornya anjlok
                            penalty = 15 * word_diff
                        
                        subset_score = score_set - penalty
                        adjusted = max(adjusted, subset_score)
                else:
                    adjusted = SequenceMatcher(None, part_lower, cand_lower).ratio() * 100.0
                    word_diff = abs(len(part_words) - len(cand_words))
                    if word_diff > 0:
                        adjusted -= (10 * word_diff) if not is_dki else (3 * word_diff)

                if adjusted > best_score:
                    best_score = adjusted
                    best_cand = cand

        if best_cand:
            sandi = SANDI_MAPPING.get(best_cand, "")
            jenis = JENIS_MAPPING.get(best_cand, "")
            return (best_cand, round(best_score, 1), sandi, jenis)

        return ("", 0.0, "", "")

    def apply_fuzzy_to_df(dframe: pd.DataFrame) -> pd.DataFrame:
        if dframe.empty:
            # Ensure columns exist and in the expected order
            for c in ["Sandi", "Jenis Penyelenggara", "Matched Name", "Match Perc"]:
                if c not in dframe.columns:
                    dframe[c] = pd.Series(dtype=object)
            return dframe
            
        # Panggil fungsi fuzzy dengan melemparkan Nilai KPwBI (kolom C)
        def fuzzy_wrapper(row):
            kpw_val = row[col_c_name] if col_c_name in row.index else ""
            return pd.Series(lookup_fuzzy(row["_temp_lookup_key"], kpw_val))
            
        fuzzy_series = dframe.apply(fuzzy_wrapper, axis=1)
        fuzzy_series.columns = ["Matched Name", "Match Perc", "Sandi", "Jenis Penyelenggara"]
        
        # Merge back
        dframe = dframe.reset_index(drop=True).join(fuzzy_series.reset_index(drop=True))

        # Reorder: place Sandi and Jenis Penyelenggara after Nama Penyelenggara, and Match Perc last
        cols = [c for c in dframe.columns if c != "Match Perc"]
        if "Nama Penyelenggara" in cols:
            base_idx = cols.index("Nama Penyelenggara") + 1
            # remove these if present
            for c in ["Sandi", "Jenis Penyelenggara", "Matched Name"]:
                if c in cols:
                    cols.remove(c)
            # insert desired order
            for i, c in enumerate(["Sandi", "Jenis Penyelenggara", "Matched Name"]):
                cols.insert(base_idx + i, c)
        # ensure Match Perc at end
        cols.append("Match Perc")
        # Some columns may be duplicates or missing; filter by actual columns
        cols = [c for c in cols if c in dframe.columns]
        dframe = dframe[cols]
        return dframe

    df_kept = apply_fuzzy_to_df(df_kept)
    if not df_excluded.empty:
        df_excluded = apply_fuzzy_to_df(df_excluded)

    # --- Post-processing per user request ---
    # 1) Remove the 'Matched Name' column from final output
    if "Matched Name" in df_kept.columns:
        df_kept = df_kept.drop(columns=["Matched Name"])

    # 2) Drop the KPwBI / Satuan Kerja column (user requested it not be shown)
    if col_c_name and col_c_name in df_kept.columns:
        df_kept = df_kept.drop(columns=[col_c_name])

    # 3) Keep only rows with Match Perc > 94 (use original numeric score),
    # then remove the Match Perc column from the output as requested.
    if "Match Perc" in df_kept.columns:
        # Ensure numeric, fill NaN with 0
        df_kept["Match Perc"] = pd.to_numeric(df_kept["Match Perc"], errors='coerce').fillna(0)
        # Filter strictly greater than 94
        df_kept = df_kept[df_kept["Match Perc"] > 94].reset_index(drop=True)
        # Drop the Match Perc column entirely from final output
        df_kept = df_kept.drop(columns=["Match Perc"])

    # 4) Reorder columns: put `Sandi` immediately after `Nama Penyelenggara`,
    # then `Tahun Laporan`, then `Periode Laporan`, then `Jenis Penyelenggara`.
    if not df_kept.empty:
        cols = list(df_kept.columns)
        if "Nama Penyelenggara" in cols:
            new_cols = ["Nama Penyelenggara"]
            # Sandi right after Nama Penyelenggara (if present)
            if "Sandi" in cols:
                new_cols.append("Sandi")
            # Tahun Laporan next to Nama / Sandi
            if "Tahun Laporan" in cols:
                new_cols.append("Tahun Laporan")
            # Periode Laporan directly after Tahun Laporan
            if "Periode Laporan" in cols:
                new_cols.append("Periode Laporan")
            # Then Jenis Penyelenggara
            if "Jenis Penyelenggara" in cols:
                new_cols.append("Jenis Penyelenggara")
            # Append remaining columns in their original order
            for c in cols:
                if c not in new_cols:
                    new_cols.append(c)
            # Reorder dataframe
            df_kept = df_kept[new_cols]

    # Move rows from excluded -> kept if fuzzy match >= 90%
    if not df_excluded.empty:
        if "Match Perc" in df_excluded.columns:
            mask_promote = df_excluded["Match Perc"].ge(90)
            if mask_promote.any():
                to_promote = df_excluded.loc[mask_promote].copy()
                df_kept = pd.concat([df_kept, to_promote], ignore_index=True)
                df_excluded = df_excluded.loc[~mask_promote].reset_index(drop=True)

    # Clean up temp column
    df_kept = df_kept.drop(columns=["_temp_lookup_key"], errors='ignore')
    if not df_excluded.empty:
        df_excluded = df_excluded.drop(columns=["_temp_lookup_key"], errors='ignore')

    # Write single output (cleaned with matching info)
    try:
        with pd.ExcelWriter(output_clean) as writer:
            df_kept.to_excel(writer, index=False)
    except PermissionError:
        output_clean = pick_non_locked_path(output_clean)
        with pd.ExcelWriter(output_clean) as writer:
            df_kept.to_excel(writer, index=False)

    print("Selesai.")
    print(f"Output cleaned : {output_clean}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())