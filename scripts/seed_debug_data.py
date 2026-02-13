"""
Seed dummy flash_point + feed_entries tables for debug/testing.

Creates date-partitioned tables for today (or a specified date)
and populates them with realistic geopolitical flashpoints and
news feed entries — enough to test the full pipeline end-to-end.

Usage:
    python scripts/seed_debug_data.py                # uses today's date
    python scripts/seed_debug_data.py --date 2026-02-12
    python scripts/seed_debug_data.py --drop          # drop existing + recreate
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from datetime import UTC, date, datetime

import click
from sqlalchemy import text

# ── Ensure project root is on sys.path ────────────────
sys.path.insert(0, ".")

from core.db.engine import get_async_session
from core.db.table_resolver import make_table_name

# ───────────────────────────────────────────────────────
#  Sample Data
# ───────────────────────────────────────────────────────

FLASHPOINTS = [
    {
        "title": "Russia-Ukraine Conflict Escalation",
        "description": (
            "Ongoing military conflict between Russia and Ukraine, including "
            "territorial disputes in eastern Ukraine, Crimea, and international "
            "sanctions regime. Recent developments include intensified strikes "
            "on energy infrastructure and diplomatic negotiations."
        ),
        "entities": [
            "Russia",
            "Ukraine",
            "NATO",
            "European Union",
            "Volodymyr Zelenskyy",
            "Vladimir Putin",
        ],
        "domains": [
            "reuters.com",
            "bbc.com",
            "cnn.com",
            "aljazeera.com",
            "theguardian.com",
            "dw.com",
        ],
    },
    {
        "title": "Israel-Palestine Crisis",
        "description": (
            "Escalation of the Israel-Palestine conflict following October 2023 "
            "events. Humanitarian crisis in Gaza, hostage negotiations, and "
            "regional spillover affecting Lebanon, Yemen, and Red Sea shipping."
        ),
        "entities": [
            "Israel",
            "Palestine",
            "Hamas",
            "Hezbollah",
            "Benjamin Netanyahu",
            "United Nations",
        ],
        "domains": [
            "aljazeera.com",
            "timesofisrael.com",
            "bbc.com",
            "middleeasteye.net",
            "reuters.com",
        ],
    },
    {
        "title": "South China Sea Tensions",
        "description": (
            "Territorial disputes in the South China Sea involving China, "
            "Philippines, Vietnam, and other ASEAN nations. Increasing military "
            "presence, artificial island construction, and freedom of navigation "
            "operations by the United States."
        ),
        "entities": [
            "China",
            "Philippines",
            "United States",
            "ASEAN",
            "Xi Jinping",
            "Ferdinand Marcos Jr.",
        ],
        "domains": [
            "scmp.com",
            "reuters.com",
            "rappler.com",
            "nikkei.com",
            "bbc.com",
        ],
    },
    {
        "title": "Sudan Civil War",
        "description": (
            "Armed conflict between the Sudanese Armed Forces and the Rapid "
            "Support Forces. Massive displacement, humanitarian crisis, and "
            "regional instability affecting Chad, South Sudan, and Egypt."
        ),
        "entities": [
            "Sudan",
            "Rapid Support Forces",
            "Sudanese Armed Forces",
            "Abdel Fattah al-Burhan",
            "Mohamed Hamdan Dagalo",
        ],
        "domains": [
            "aljazeera.com",
            "reuters.com",
            "bbc.com",
            "dabangasudan.org",
        ],
    },
    {
        "title": "Amazon Deforestation and Climate Policy",
        "description": (
            "Environmental crisis in the Amazon rainforest involving deforestation, "
            "illegal mining, and climate policy. Brazil's role in COP agreements "
            "and enforcement of environmental protections under the Lula government."
        ),
        "entities": [
            "Brazil",
            "Amazon",
            "Lula da Silva",
            "COP30",
            "IBAMA",
            "European Union",
        ],
        "domains": [
            "g1.globo.com",
            "reuters.com",
            "theguardian.com",
            "mongabay.com",
            "climatechangenews.com",
        ],
    },
]

# Feed entries per flashpoint — realistic news articles
FEED_ENTRIES_BY_FP = {
    "Russia-Ukraine Conflict Escalation": [
        {
            "url": "https://www.aljazeera.com/news/2026/2/9/child-among-4-killed-in-latest-russian-missile-and-drone-barrage-ukraine",
            "title": "Child among 4 killed in latest Russian missile and drone barrage: Ukraine",
            "description": "Ukraine's foreign minister calls for complete EU entry ban on Russians participating in the war.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "2026-02-09",
            "image": None,
            "sample_content": (
                "KYIV, Feb 12 (Reuters) - Russian missile and drone attacks targeted Ukrainian energy "
                "facilities overnight, causing power outages across several regions including Kharkiv, "
                "Zaporizhzhia and Dnipropetrovsk, Ukrainian officials said on Wednesday. President "
                "Volodymyr Zelenskyy condemned the attacks, calling them deliberate targeting of civilian "
                "infrastructure. The European Union and NATO issued statements urging Russia to cease "
                "strikes on energy infrastructure ahead of winter. Ukraine's energy minister said emergency "
                "repairs were underway but warned rolling blackouts could last several days. The attacks "
                "come as diplomatic efforts to negotiate a ceasefire have stalled, with both Moscow and "
                "Kyiv setting preconditions that the other side has rejected."
            ),
        },
        {
            "url": "https://www.aljazeera.com/news/2026/2/7/us-has-given-ukraine-and-russia-june-deadline-to-end-war-zelenskyy",
            "title": "US has given Ukraine and Russia June deadline to end war: Zelenskyy",
            "description": "Ukraine's president says the US has also proposed new trilateral talks in Miami, which he said Ukraine will attend.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "2026-02-07",
            "image": None,
            "sample_content": (
                "As diplomatic efforts intensify to end the war in Ukraine, the positions of both Russia "
                "and Ukraine remain far apart. Moscow demands recognition of territorial gains in Donetsk, "
                "Luhansk, Zaporizhzhia and Kherson oblasts, as well as a guarantee that Ukraine will not "
                "join NATO. Kyiv insists on full territorial integrity and security guarantees from Western "
                "allies. France and Germany have proposed a phased approach, while the United States has "
                "signaled willingness to mediate directly. Analysts say the gap between the two sides "
                "remains significant, but the economic toll of the conflict is creating pressure on both "
                "governments to find a resolution."
            ),
        },
        {
            "url": "https://www.dw.com/en/life-in-kyiv-working-through-war-and-blackouts/video-75846445",
            "title": "Life in Kyiv: Working through war and blackouts",
            "description": "Daily life in Kyiv continues under the shadow of war and rolling power outages from Russian strikes on energy infrastructure.",
            "domain": "dw.com",
            "language": "en",
            "sourcecountry": "Germany",
            "seendate": "2026-02-11",
            "image": None,
            "sample_content": (
                "NATO defence ministers agreed on Tuesday to deploy additional troops and equipment to "
                "Poland, Romania and the Baltic states. The alliance's Secretary General said the move "
                "was a direct response to continued Russian aggression in Ukraine. Germany will contribute "
                "a permanent brigade of 4,000 troops to Lithuania, while the United Kingdom has pledged "
                "additional air defence systems to Estonia. The decision comes ahead of a NATO summit in "
                "The Hague where leaders will discuss long-term defence spending targets."
            ),
        },
        {
            "url": "https://www.theguardian.com/world/2026/feb/12/ukraine-war-briefing-elections-only-after-ceasefire-zelenskyy",
            "title": "Ukraine war briefing: Elections will be held only after ceasefire, says Zelenskyy",
            "description": "Latest developments in the Russia-Ukraine war including Zelenskyy's statement on elections and a ceasefire.",
            "domain": "theguardian.com",
            "language": "en",
            "sourcecountry": "United Kingdom",
            "seendate": "2026-02-12",
            "image": None,
            "sample_content": (
                "Запоріжжя зазнало чергових обстрілів з боку російських збройних сил. За даними "  # noqa: RUF001
                "обласної військової адміністрації, було пошкоджено житлові будинки та об'єкти "  # noqa: RUF001
                "інфраструктури. Евакуація мирних жителів з прифронтових територій триває. "
                "Президент Зеленський закликав міжнародну спільноту посилити тиск на Росію. "
                "Європейський Союз оголосив про додаткову гуманітарну допомогу для постраждалих регіонів."
            ),
        },
        {
            "url": "https://www.aljazeera.com/features/2026/2/6/ukraine-pulls-plug-on-russian-starlink-beefs-up-drone-defence",
            "title": "Ukraine pulls plug on Russian Starlink, beefs up drone defence",
            "description": "Ukraine responded to renewed Russian strikes on energy infrastructure, logistics far from front lines.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "2026-02-06",
            "image": None,
            "sample_content": (
                "Министерство иностранных дел России выступило с заявлением в ответ на новый пакет "  # noqa: RUF001
                "санкций Европейского союза. Официальный представитель МИД Мария Захарова назвала "
                "санкции 'контрпродуктивными' и пообещала ответные меры. Новые ограничения затрагивают "
                "несколько российских компаний в энергетическом и банковском секторах. Россия заявила, "
                "что продолжит развивать торговые отношения с Китаем, Индией и другими партнёрами."  # noqa: RUF001
            ),
        },
    ],
    "Israel-Palestine Crisis": [
        {
            "url": "https://www.aljazeera.com/news/2026/2/3/global-conflicts-pushing-humanitarian-law-to-breaking-point-report-warns",
            "title": "Global conflicts pushing humanitarian law to breaking point, report warns",
            "description": "As armed groups target civilians unchecked, Geneva Academy warn of humanitarian law's collapse and its ramifications.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "2026-02-03",
            "image": None,
            "sample_content": (
                "The United Nations has issued an urgent warning about the deteriorating humanitarian "
                "situation in Gaza. UNRWA and the World Food Programme reported that aid deliveries "
                "have dropped to their lowest levels since the conflict began. More than 1.5 million "
                "displaced Palestinians are facing acute food insecurity, with children particularly "
                "at risk of malnutrition. The Israeli government has said it is facilitating aid "
                "deliveries but cited security concerns for inspection delays. Egypt and Jordan have "
                "called for an immediate humanitarian corridor to be established."
            ),
        },
        {
            "url": "https://www.aljazeera.com/news/2026/2/8/russia-says-second-suspect-in-generals-shooting-arrested-in-dubai",
            "title": "Russia says second suspect in general's shooting arrested in Dubai",
            "description": "Lyubomir Korba is extradited from the UAE, where he fled hours after Friday's attack, Russia's FSB says.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "2026-02-08",
            "image": None,
            "sample_content": (
                "Negotiations to secure the release of hostages held by Hamas in Gaza have entered "
                "a critical phase, according to Israeli officials familiar with the talks. Qatar "
                "and Egypt are mediating between the two sides, with the United States providing "
                "behind-the-scenes support. Prime Minister Benjamin Netanyahu has said Israel will "
                "not agree to a permanent ceasefire until all hostages are returned. Hamas has "
                "demanded the release of Palestinian prisoners and a full withdrawal from Gaza "
                "as conditions for any deal."
            ),
        },
        {
            "url": "https://www.aljazeera.com/news/2026/2/6/just-how-excellent-was-trump-and-xi-jinpings-phone-call-really",
            "title": "Just how 'excellent' was Trump and Xi Jinping's phone call, really?",
            "description": "US president raved about his extremely good personal relationship with the Chinese leader, but Xi was more muted.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "2026-02-06",
            "image": None,
            "sample_content": (
                "Yemen's Houthi rebels have continued their attacks on commercial shipping in the Red "
                "Sea, forcing major carriers including Maersk and MSC to reroute vessels around the Cape "
                "of Good Hope. The disruptions have added an estimated 10-14 days to shipping times "
                "between Asia and Europe, increasing costs for global supply chains. The Suez Canal "
                "Authority reported a 40% decline in transit revenue. The United States and United "
                "Kingdom have conducted joint military strikes against Houthi positions in Yemen, "
                "but the attacks on shipping have continued."
            ),
        },
        {
            "url": "https://www.aljazeera.com/news/2026/2/9/russia-ukraine-war-list-of-key-events-day-1446",
            "title": "Russia-Ukraine war: List of key events, day 1,446",
            "description": "These are the key developments from day 1,446 of Russia's war on Ukraine.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "2026-02-09",
            "image": None,
            "sample_content": (
                "تستمر المفاوضات الدبلوماسية بين الأطراف المتنازعة في قطاع غزة. وقد أعلنت "
                "مصر وقطر عن تقدم في المحادثات الرامية إلى التوصل لوقف إطلاق النار. وطالبت "
                "حركة حماس بانسحاب القوات الإسرائيلية من غزة كشرط أساسي. فيما أكدت إسرائيل "
                "على ضرورة إطلاق سراح جميع المحتجزين. وقد دعت الأمم المتحدة جميع الأطراف "
                "إلى ضبط النفس وحماية المدنيين."
            ),
        },
    ],
    "South China Sea Tensions": [
        {
            "url": "https://www.aljazeera.com/news/2026/1/29/dozens-killed-in-rsf-drone-attack-in-war-torn-sudans-south-kordofan",
            "title": "Dozens killed in RSF drone attack in war-torn Sudan's South Kordofan",
            "description": "The attack in Dilling town comes a day after Sudan's military declared an end to RSF siege there.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "2026-01-29",
            "image": None,
            "sample_content": (
                "China Coast Guard vessels conducted what Beijing described as routine patrols near "
                "Scarborough Shoal in the South China Sea on Tuesday. The Philippines condemned the "
                "action, saying the vessels entered waters within Manila's exclusive economic zone. "
                "President Ferdinand Marcos Jr. called the patrols a provocative escalation and "
                "summoned the Chinese ambassador. Beijing's foreign ministry spokesperson said China "
                "has indisputable sovereignty over the South China Sea islands and adjacent waters. "
                "ASEAN foreign ministers issued a joint statement calling for restraint from all parties."
            ),
        },
        {
            "url": "https://www.aljazeera.com/news/2026/2/3/sudan-military-claims-to-break-siege-of-key-kordofan-city-of-kadugli",
            "title": "Sudan military claims to break siege of key Kordofan city of Kadugli",
            "description": "Government forces have entered South Kordofan's capital, Kadugli, days after breaking a siege in nearby Dilling.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "2026-02-03",
            "image": None,
            "sample_content": (
                "The Philippine Coast Guard reported a tense standoff with Chinese maritime militia "
                "vessels near Ayungin Shoal, also known as Second Thomas Shoal, in the Spratly Islands. "
                "Philippine boats were conducting a routine resupply mission to the BRP Sierra Madre "
                "when Chinese vessels attempted to block their path using water cannons. The Philippine "
                "military said no personnel were injured. The United States reaffirmed its mutual "
                "defence treaty obligations with the Philippines."
            ),
        },
        {
            "url": "https://www.aljazeera.com/news/longform/2026/2/3/drone-warfare-in-sudan-tracking-1000-aerial-attacks-since-april-2023",
            "title": "The drones being used in Sudan: 1,000 attacks since April 2023",
            "description": "A visual analysis into the types of drones, sourcing methods, attack locations and the human toll of aerial warfare.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "2026-02-03",
            "image": None,
            "sample_content": (
                "The US Navy destroyer USS Milius conducted a freedom of navigation operation through "
                "the South China Sea near the Paracel Islands on Monday. The US Seventh Fleet said "
                "the transit challenged China's excessive maritime claims that restrict navigation "
                "rights. China's People's Liberation Army Southern Theater Command said it tracked "
                "and warned the vessel. Japan and Australia expressed support for the operation, "
                "emphasizing the importance of a rules-based international order in the Indo-Pacific."
            ),
        },
    ],
    "Sudan Civil War": [
        {
            "url": "https://www.aljazeera.com/news/2026/1/31/like-judgment-day-sudanese-doctor-recounts-escape-from-el-fasher",
            "title": "'Like judgement day': Sudanese doctor recounts escape from el-Fasher",
            "description": "Physician who fled the city's last functioning hospital recounts RSF assault on the capital of North Darfur province.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "2026-01-31",
            "image": None,
            "sample_content": (
                "Fighting between the Sudanese Armed Forces and the Rapid Support Forces has intensified "
                "around El Fasher, the capital of North Darfur. The United Nations reported that "
                "hundreds of families have been displaced by the latest round of clashes. General Abdel "
                "Fattah al-Burhan, the head of the Sudanese military, vowed to retake the city from RSF "
                "control. Mohamed Hamdan Dagalo, the RSF commander known as Hemedti, accused the army "
                "of indiscriminate shelling. Médecins Sans Frontières said hospitals in the area are "
                "overwhelmed and running out of medical supplies."
            ),
        },
        {
            "url": "https://www.aljazeera.com/news/2026/2/1/khartoum-airport-receives-first-scheduled-flight-since-start-of-sudan-war",
            "title": "Khartoum airport receives first scheduled flight since start of Sudan war",
            "description": "Celebrations as flight carries dozens of passengers from Port Sudan to Sudanese capital.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "2026-02-01",
            "image": None,
            "sample_content": (
                "The UN refugee agency UNHCR has reported a massive surge in Sudanese refugees crossing "
                "into eastern Chad as violence spreads in West Darfur. More than 15,000 people have "
                "arrived at the Adré border crossing in the past week alone. Refugee camps in Chad are "
                "already at capacity, with the Ourang camp housing three times its intended population. "
                "UNHCR spokesperson Shabia Mantoo called for urgent international support and funding. "
                "South Sudan and Egypt are also seeing increased refugee flows from Sudan."
            ),
        },
        {
            "url": "https://www.aljazeera.com/news/2026/1/28/sudans-war-displaced-crisis-peaks-as-millions-eye-return-to-ruined-homes",
            "title": "Sudan's war displaced crisis peaks as millions eye return to ruined homes",
            "description": "Millions of Sudanese displaced by the civil war face returning to destroyed homes and infrastructure.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "2026-01-28",
            "image": None,
            "sample_content": (
                "La situation humanitaire au Soudan continue de se détériorer, selon les Nations Unies. "
                "Plus de huit millions de personnes ont été déplacées depuis le début du conflit entre "
                "les Forces armées soudanaises et les Forces de soutien rapide. Le Programme alimentaire "
                "mondial a averti que des millions de Soudanais risquent la famine. L'Union africaine "
                "et la France ont appelé à un cessez-le-feu immédiat. Le Conseil de sécurité de l'ONU "
                "doit se réunir pour discuter de la crise cette semaine."
            ),
        },
    ],
    "Amazon Deforestation and Climate Policy": [
        {
            "url": "https://www.theguardian.com/environment/2025/nov/06/climate-crisis-amazon-lakes-water-temperature-hotter-than-spa-bath-aoe",
            "title": "Amazon lakes hit 'unbearable' hot-tub temperatures amid mass die-offs of pink river dolphins",
            "description": "As the climate crisis worsens, Amazon lakes are reaching temperatures that threaten aquatic ecosystems.",
            "domain": "theguardian.com",
            "language": "en",
            "sourcecountry": "United Kingdom",
            "seendate": "2025-11-06",
            "image": None,
            "sample_content": (
                "O Instituto Chico Mendes de Conservação da Biodiversidade (ICMBio) confirmou que o "
                "desmatamento na Amazônia brasileira atingiu o menor nível dos últimos nove anos. A "
                "redução de 45% em relação ao ano anterior é atribuída às políticas de fiscalização "
                "do governo do presidente Lula da Silva e ao fortalecimento do IBAMA. Apesar dos "
                "avanços, organizações ambientais alertam que a mineração ilegal continua sendo uma "
                "ameaça significativa em áreas indígenas nos estados do Pará e Roraima."
            ),
        },
        {
            "url": "https://www.theguardian.com/environment/2025/nov/08/brazil-amazon-summit-juggles-climate-and-social-priorities",
            "title": "Lula's balancing act: Cop30 Amazon summit juggles climate and social priorities",
            "description": "Brazil faces scrutiny over its environmental record as it hosts COP30 in the Amazon city of Belém.",
            "domain": "theguardian.com",
            "language": "en",
            "sourcecountry": "United Kingdom",
            "seendate": "2025-11-08",
            "image": None,
            "sample_content": (
                "Brazil is preparing to host COP30 in the Amazon city of Belém later this year, in "
                "what environmental groups are calling the most important climate summit since Paris. "
                "President Lula da Silva has pledged to showcase Brazil's progress on deforestation "
                "reduction, but critics point to continued illegal mining and the slow pace of "
                "enforcement. The European Union has warned that its deforestation regulation will "
                "affect Brazilian agricultural exports. COP30 is expected to attract over 40,000 "
                "delegates and focus on tropical forest preservation and carbon markets."
            ),
        },
        {
            "url": "https://www.dw.com/en/south-africas-ramaphosa-to-deploy-army-to-combat-crime/a-75941458",
            "title": "South Africa's Ramaphosa to deploy army to combat crime",
            "description": "President Ramaphosa announces military deployment to combat rising crime in South Africa.",
            "domain": "dw.com",
            "language": "en",
            "sourcecountry": "Germany",
            "seendate": "2026-02-12",
            "image": None,
            "sample_content": (
                "Environmental groups have reported a surge in illegal gold mining operations, known as "
                "garimpo, in protected indigenous territories in the Brazilian Amazon. The Yanomami and "
                "Munduruku indigenous lands in Pará and Roraima states have been particularly affected, "
                "with satellite imagery showing new mining sites along major rivers. IBAMA, Brazil's "
                "environmental enforcement agency, has conducted raids but says it lacks resources "
                "for sustained enforcement. Mercury contamination from mining is poisoning waterways "
                "and affecting indigenous communities' health."
            ),
        },
        {
            "url": "https://www.dw.com/en/france-germany-signal-unity-at-eus-belgium-castle-retreat/a-75938810",
            "title": "France, Germany signal unity at EU's Belgium castle retreat",
            "description": "France and Germany signal closer cooperation at an EU leaders retreat in Belgium.",
            "domain": "dw.com",
            "language": "en",
            "sourcecountry": "Germany",
            "seendate": "2026-02-12",
            "image": None,
            "sample_content": (
                "A Aprosoja Brasil e organizações ambientais estão debatendo a renovação da Moratória "
                "da Soja na Amazônia. O acordo, que proíbe a comercialização de soja cultivada em áreas "
                "desmatadas após 2008, tem sido fundamental para reduzir o desmatamento ligado à "
                "agricultura. Produtores argumentam que a moratória limita a competitividade do setor, "
                "enquanto ambientalistas defendem sua manutenção. O governo Lula sinalizou apoio à "
                "renovação do acordo como parte de seus compromissos climáticos para a COP30 em Belém."
            ),
        },
    ],
}


# ───────────────────────────────────────────────────────
#  Table Creation + Seeding
# ───────────────────────────────────────────────────────


async def create_flash_point_table(session, table_name: str, drop: bool = False):
    """Create flash_point_YYYYMMDD with exact upstream schema."""
    if drop:
        await session.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))

    await session.execute(
        text(f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            title       TEXT NOT NULL,
            description TEXT NOT NULL,
            entities    JSONB,
            domains     JSONB,
            run_id      TEXT,
            created_at  TIMESTAMPTZ DEFAULT NOW(),
            updated_at  TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    )
    await session.commit()
    print(f"  ✓ Created table: {table_name}")


async def create_feed_entries_table(session, table_name: str, fp_table: str, drop: bool = False):
    """Create feed_entries_YYYYMMDD with exact upstream schema."""
    if drop:
        await session.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))

    await session.execute(
        text(f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            flashpoint_id       UUID REFERENCES "{fp_table}"(id) ON DELETE CASCADE,
            url                 TEXT,
            title               TEXT,
            seendate            TEXT,
            domain              TEXT,
            language            TEXT,
            sourcecountry       TEXT,
            description         TEXT,
            image               TEXT,
            title_en            TEXT,
            images              TEXT[] DEFAULT '{{}}',
            hostname            TEXT,
            content             TEXT,
            compressed_content  TEXT,
            summary             TEXT,
            entities            JSONB,
            geo_entities        JSONB,
            created_at          TIMESTAMPTZ DEFAULT NOW(),
            updated_at          TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    )
    await session.commit()
    print(f"  ✓ Created table: {table_name}")


async def seed_flashpoints(session, table_name: str) -> dict[str, uuid.UUID]:
    """Insert flashpoints and return {title: id} mapping."""
    import json

    fp_map: dict[str, uuid.UUID] = {}
    now = datetime.now(UTC)

    for fp in FLASHPOINTS:
        fp_id = uuid.uuid4()
        fp_map[fp["title"]] = fp_id

        await session.execute(
            text(f"""
                INSERT INTO "{table_name}" (id, title, description, entities, domains, run_id, created_at, updated_at)
                VALUES (:id, :title, :description, CAST(:entities AS jsonb), CAST(:domains AS jsonb), :run_id, :created_at, :updated_at)
            """),
            {
                "id": fp_id,
                "title": fp["title"],
                "description": fp["description"],
                "entities": json.dumps(fp["entities"]),
                "domains": json.dumps(fp["domains"]),
                "run_id": "debug-seed-001",
                "created_at": now,
                "updated_at": now,
            },
        )

    await session.commit()
    print(f"  ✓ Inserted {len(fp_map)} flashpoints")
    return fp_map


async def seed_feed_entries(
    session, table_name: str, fp_map: dict[str, uuid.UUID], with_content: bool = False
) -> int:
    """Insert feed entries linked to flashpoints. Returns count.

    Args:
        with_content: If True, pre-fill the content field with sample article text
                      so the enrichment pipeline can be tested without fetching URLs.
                      Entries with content filled will be skipped by the pipeline's
                      "unprocessed" filter (content IS NULL), so use this for testing
                      NER/geo/translate directly, not for testing the full fetch pipeline.
    """
    total = 0

    for fp_title, entries in FEED_ENTRIES_BY_FP.items():
        fp_id = fp_map.get(fp_title)
        if not fp_id:
            print(f"  ⚠ Flashpoint not found: {fp_title}")
            continue

        for entry in entries:
            content = entry.get("sample_content") if with_content else None

            await session.execute(
                text(f"""
                    INSERT INTO "{table_name}" (
                        flashpoint_id, url, title, seendate, domain,
                        language, sourcecountry, description, image, content
                    )
                    VALUES (
                        :flashpoint_id, :url, :title, :seendate, :domain,
                        :language, :sourcecountry, :description, :image, :content
                    )
                """),
                {
                    "flashpoint_id": fp_id,
                    "url": entry["url"],
                    "title": entry["title"],
                    "seendate": entry["seendate"],
                    "domain": entry["domain"],
                    "language": entry["language"],
                    "sourcecountry": entry["sourcecountry"],
                    "description": entry["description"],
                    "image": entry.get("image"),
                    "content": content,
                },
            )
            total += 1

    await session.commit()
    mode = "WITH sample content" if with_content else "without content (unprocessed)"
    print(f"  ✓ Inserted {total} feed entries ({mode})")
    return total


async def verify_data(session, fp_table: str, fe_table: str):
    """Print summary of seeded data."""
    print("\n── Verification ────────────────────────────────")

    # Flashpoints
    result = await session.execute(text(f'SELECT COUNT(*) FROM "{fp_table}"'))
    fp_count = result.scalar()
    print(f"  {fp_table}: {fp_count} flashpoints")

    result = await session.execute(text(f'SELECT id, title FROM "{fp_table}" ORDER BY created_at'))
    for row in result.fetchall():
        print(f"    • [{str(row[0])[:8]}…] {row[1]}")

    # Feed entries
    result = await session.execute(text(f'SELECT COUNT(*) FROM "{fe_table}"'))
    fe_count = result.scalar()
    print(f"\n  {fe_table}: {fe_count} entries")

    result = await session.execute(
        text(f"""
        SELECT fe.language, COUNT(*) as cnt
        FROM "{fe_table}" fe
        GROUP BY fe.language
        ORDER BY cnt DESC
    """)
    )
    print(f"  Languages: {dict(result.fetchall())}")

    # Unprocessed (content IS NULL)
    result = await session.execute(
        text(f"""
        SELECT COUNT(*) FROM "{fe_table}"
        WHERE flashpoint_id IS NOT NULL AND content IS NULL
    """)
    )
    unprocessed = result.scalar()
    print(f"  Unprocessed (content IS NULL): {unprocessed}")

    print(
        f"\n  ✓ Ready for pipeline! Run with --date {fp_table.split('_')[-1][:4]}-{fp_table.split('_')[-1][4:6]}-{fp_table.split('_')[-1][6:]}"
    )


# ───────────────────────────────────────────────────────
#  CLI
# ───────────────────────────────────────────────────────


@click.command()
@click.option(
    "--date", "target_date", default=None, help="Date suffix (YYYY-MM-DD). Default: today"
)
@click.option("--drop", is_flag=True, help="Drop existing tables before creating")
@click.option(
    "--with-content",
    is_flag=True,
    help="Pre-fill content with sample text (for testing enrichment without fetching URLs)",
)
def cli(target_date: str | None, drop: bool, with_content: bool):
    """Seed debug flash_point + feed_entries tables."""
    asyncio.run(_main(target_date, drop, with_content))


async def _main(target_date_str: str | None, drop: bool, with_content: bool = False):
    target = date.fromisoformat(target_date_str) if target_date_str else date.today()

    fp_table = make_table_name("flash_point", target)
    fe_table = make_table_name("feed_entries", target)

    content_mode = "pre-filled" if with_content else "NULL (unprocessed)"
    print("╔══════════════════════════════════════════════╗")
    print("║  MASX-GSGI Debug Data Seeder                ║")
    print("╠══════════════════════════════════════════════╣")
    print(f"║  Date:    {target}                         ║")
    print(f"║  FP tbl:  {fp_table:<33}║")
    print(f"║  FE tbl:  {fe_table:<33}║")
    print(f"║  Drop:    {drop!s:<33}║")
    print(f"║  Content: {content_mode:<33}║")
    print("╚══════════════════════════════════════════════╝")
    print()

    session_factory = get_async_session()

    async with session_factory() as session:
        # 1. Create tables
        print("1. Creating tables...")
        await create_flash_point_table(session, fp_table, drop=drop)
        await create_feed_entries_table(session, fe_table, fp_table, drop=drop)

        # 2. Check if data already exists
        result = await session.execute(text(f'SELECT COUNT(*) FROM "{fp_table}"'))
        existing = result.scalar()
        if existing and existing > 0 and not drop:
            print(f"\n  ⚠ Tables already have data ({existing} flashpoints). Use --drop to reseed.")
            await verify_data(session, fp_table, fe_table)
            return

        # 3. Seed flashpoints
        print("\n2. Seeding flashpoints...")
        fp_map = await seed_flashpoints(session, fp_table)

        # 4. Seed feed entries
        print("\n3. Seeding feed entries...")
        await seed_feed_entries(session, fe_table, fp_map, with_content=with_content)

        # 5. Verify
        await verify_data(session, fp_table, fe_table)

    print("\n✓ Done!")


if __name__ == "__main__":
    cli()
