"""Generate brand assets for the TechnoSearch/RAG Catalog project."""

from __future__ import annotations

import shutil
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont


ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "assets" / "brand"
PNG = ASSETS / "png"
SVG = ASSETS / "svg"
ICO = ASSETS / "ico"
BACKGROUNDS = ASSETS / "backgrounds"
AVATARS = ASSETS / "avatars"
SHORTCUTS = ASSETS / "shortcuts"
SOURCE = ASSETS / "source"

COLORS = {
    "graphite": "#17212B",
    "graphite_2": "#25313D",
    "slate": "#5F6B78",
    "line": "#D7E0E8",
    "paper": "#F7FAFC",
    "white": "#FFFFFF",
    "amber": "#F2B625",
    "amber_dark": "#D98B00",
    "blue": "#157FC4",
    "blue_dark": "#0B5C91",
    "green": "#2E8B57",
}


def ensure_dirs() -> None:
    for path in (PNG, SVG, ICO, BACKGROUNDS, AVATARS, SHORTCUTS, SOURCE):
        path.mkdir(parents=True, exist_ok=True)


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    candidates = [
        Path("C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf"),
        Path("C:/Windows/Fonts/segoeuib.ttf" if bold else "C:/Windows/Fonts/segoeui.ttf"),
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size)
    return ImageFont.load_default()


def hex_to_rgba(value: str, alpha: int = 255) -> tuple[int, int, int, int]:
    value = value.lstrip("#")
    return tuple(int(value[i : i + 2], 16) for i in (0, 2, 4)) + (alpha,)


def gradient(size: tuple[int, int], start: str, end: str, vertical: bool = False) -> Image.Image:
    w, h = size
    img = Image.new("RGBA", size)
    px = img.load()
    a = hex_to_rgba(start)
    b = hex_to_rgba(end)
    span = h - 1 if vertical else w - 1
    for y in range(h):
        for x in range(w):
            t = (y if vertical else x) / max(span, 1)
            px[x, y] = tuple(round(a[i] * (1 - t) + b[i] * t) for i in range(4))
    return img


def draw_folder(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], scale: float = 1.0) -> None:
    x1, y1, x2, y2 = box
    tab_h = int((y2 - y1) * 0.23)
    tab_w = int((x2 - x1) * 0.35)
    r = int(18 * scale)
    draw.rounded_rectangle((x1, y1 + tab_h, x2, y2), radius=r, fill=COLORS["amber"])
    draw.rounded_rectangle((x1, y1, x1 + tab_w, y1 + tab_h * 2), radius=r, fill=COLORS["amber"])
    draw.polygon(
        [
            (x1 + tab_w - int(20 * scale), y1 + tab_h),
            (x1 + tab_w + int(110 * scale), y1 + tab_h),
            (x1 + tab_w + int(130 * scale), y1 + tab_h * 2),
            (x1 + tab_w - int(10 * scale), y1 + tab_h * 2),
        ],
        fill=COLORS["amber"],
    )
    draw.rounded_rectangle((x1 + 14, y1 + tab_h + 12, x2 - 14, y2 - 12), radius=r, outline=COLORS["white"], width=max(5, int(7 * scale)))


def draw_excavator(draw: ImageDraw.ImageDraw, x: int, y: int, s: float, color: str = "graphite_2") -> None:
    c = COLORS[color]
    width = max(5, int(8 * s))
    draw.rounded_rectangle((x + 88 * s, y + 72 * s, x + 194 * s, y + 112 * s), radius=int(8 * s), outline=c, width=width)
    draw.rounded_rectangle((x + 125 * s, y + 28 * s, x + 166 * s, y + 76 * s), radius=int(5 * s), outline=c, width=width)
    draw.line((x + 48 * s, y + 70 * s, x + 100 * s, y + 16 * s, x + 158 * s, y + 42 * s), fill=c, width=width)
    draw.line((x + 47 * s, y + 70 * s, x + 20 * s, y + 126 * s), fill=c, width=width)
    draw.arc((x - 10 * s, y + 112 * s, x + 58 * s, y + 164 * s), start=185, end=350, fill=c, width=width)
    draw.rounded_rectangle((x + 70 * s, y + 116 * s, x + 220 * s, y + 142 * s), radius=int(12 * s), outline=c, width=width)
    for cx in (100, 150, 200):
        draw.ellipse((x + (cx - 8) * s, y + 124 * s, x + (cx + 8) * s, y + 140 * s), fill=c)


def draw_document(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], scale: float = 1.0) -> None:
    x1, y1, x2, y2 = box
    fold = int((x2 - x1) * 0.18)
    draw.polygon([(x1, y1), (x2 - fold, y1), (x2, y1 + fold), (x2, y2), (x1, y2)], fill=COLORS["paper"], outline=COLORS["slate"])
    draw.line((x2 - fold, y1, x2 - fold, y1 + fold, x2, y1 + fold), fill=COLORS["slate"], width=max(3, int(4 * scale)))
    for i in range(5):
        yy = y1 + int((44 + i * 36) * scale)
        draw.line((x1 + int(32 * scale), yy, x2 - int(42 * scale), yy), fill="#81909E", width=max(3, int(4 * scale)))
    cx, cy = x1 + int(76 * scale), y1 + int(55 * scale)
    draw.ellipse((cx - 24 * scale, cy - 24 * scale, cx + 24 * scale, cy + 24 * scale), fill=COLORS["amber_dark"])
    draw.ellipse((cx - 10 * scale, cy - 10 * scale, cx + 10 * scale, cy + 10 * scale), fill=COLORS["white"])


def draw_mark(size: int, transparent: bool = True, badge: bool = False) -> Image.Image:
    scale = size / 1024
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0) if transparent else hex_to_rgba(COLORS["paper"]))
    draw = ImageDraw.Draw(img)

    if badge:
        draw.ellipse((24 * scale, 24 * scale, size - 24 * scale, size - 24 * scale), fill=COLORS["graphite"])
        draw.ellipse((56 * scale, 56 * scale, size - 56 * scale, size - 56 * scale), fill="#1E2A35")

    shadow = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    sh = ImageDraw.Draw(shadow)
    sh.ellipse((145 * scale, 665 * scale, 830 * scale, 810 * scale), fill=(10, 20, 30, 90))
    img.alpha_composite(shadow.filter(ImageFilter.GaussianBlur(26 * scale)))

    draw_folder(draw, (210 * scale, 265 * scale, 785 * scale, 630 * scale), scale)
    draw_document(draw, (360 * scale, 145 * scale, 655 * scale, 565 * scale), scale)
    draw_excavator(draw, int(318 * scale), int(415 * scale), scale * 1.12)

    lens_box = (555 * scale, 420 * scale, 838 * scale, 703 * scale)
    draw.ellipse(lens_box, fill=COLORS["white"], outline=COLORS["blue"], width=max(14, int(26 * scale)))
    draw.line((760 * scale, 650 * scale, 900 * scale, 790 * scale), fill=COLORS["blue"], width=max(28, int(48 * scale)))
    draw.line((780 * scale, 630 * scale, 914 * scale, 764 * scale), fill=COLORS["blue_dark"], width=max(10, int(18 * scale)))
    draw.arc((612 * scale, 475 * scale, 730 * scale, 590 * scale), 160, 265, fill=COLORS["slate"], width=max(7, int(12 * scale)))
    return img


def save_png_variants(base: Image.Image, stem: str, sizes: list[int], target: Path = PNG) -> None:
    for size in sizes:
        resized = base.resize((size, size), Image.Resampling.LANCZOS)
        resized.save(target / f"{stem}-{size}.png")


def text_size(draw: ImageDraw.ImageDraw, text: str, fnt: ImageFont.ImageFont) -> tuple[int, int]:
    box = draw.textbbox((0, 0), text, font=fnt)
    return box[2] - box[0], box[3] - box[1]


def logo_horizontal(width: int = 1600, height: int = 520) -> Image.Image:
    img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    mark = draw_mark(430, transparent=True)
    img.alpha_composite(mark, (55, 40))
    draw = ImageDraw.Draw(img)
    title = "ТЕХНОПОИСК"
    subtitle = "ПОИСК ДОКУМЕНТОВ И ДАННЫХ"
    title_font = font(126, True)
    sub_font = font(39, True)
    x = 520
    y = 152
    draw.text((x + 4, y + 6), title, font=title_font, fill=(0, 0, 0, 70))
    split = "ТЕХНО"
    split_w, _ = text_size(draw, split, title_font)
    draw.text((x, y), split, font=title_font, fill=COLORS["amber_dark"])
    draw.text((x + split_w, y), "ПОИСК", font=title_font, fill=COLORS["graphite"])
    draw.line((x, y + 164, x + 90, y + 164), fill=COLORS["blue"], width=7)
    draw.text((x + 115, y + 136), subtitle, font=sub_font, fill=COLORS["slate"])
    return img


def logo_stacked(size: int = 1200) -> Image.Image:
    img = Image.new("RGBA", (size, size), hex_to_rgba("#F6F8FA"))
    draw = ImageDraw.Draw(img)
    mark = draw_mark(720, transparent=True)
    img.alpha_composite(mark, (240, 90))
    title_font = font(108, True)
    sub_font = font(35, True)
    title = "ТЕХНОПОИСК"
    subtitle = "ПОИСК ДОКУМЕНТОВ И ДАННЫХ"
    tw, th = text_size(draw, title, title_font)
    split_w, _ = text_size(draw, "ТЕХНО", title_font)
    x = (size - tw) // 2
    y = 760
    draw.text((x, y), "ТЕХНО", font=title_font, fill=COLORS["amber_dark"])
    draw.text((x + split_w, y), "ПОИСК", font=title_font, fill=COLORS["graphite"])
    sw, _ = text_size(draw, subtitle, sub_font)
    draw.text(((size - sw) // 2, y + th + 26), subtitle, font=sub_font, fill=COLORS["slate"])
    return img


def bot_avatar() -> Image.Image:
    img = Image.new("RGBA", (1024, 1024), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.ellipse((44, 44, 980, 980), fill=COLORS["blue"])
    draw.ellipse((88, 88, 936, 936), fill=COLORS["white"])
    mark = draw_mark(760, transparent=True)
    img.alpha_composite(mark, (132, 110))
    draw.rounded_rectangle((316, 762, 708, 850), radius=30, fill=COLORS["graphite"])
    label_font = font(52, True)
    label = "БОТ"
    tw, th = text_size(draw, label, label_font)
    draw.text(((1024 - tw) // 2, 776), label, font=label_font, fill=COLORS["white"])
    return img


def user_avatar(role: str, fill: str, initials: str) -> Image.Image:
    img = Image.new("RGBA", (512, 512), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.ellipse((18, 18, 494, 494), fill=fill)
    draw.ellipse((52, 52, 460, 460), fill=COLORS["white"])
    draw.ellipse((190, 126, 322, 258), fill=fill)
    draw.rounded_rectangle((128, 292, 384, 394), radius=50, fill=fill)
    f = font(56, True)
    tw, _ = text_size(draw, initials, f)
    draw.text(((512 - tw) // 2, 410), initials, font=f, fill=COLORS["graphite"])
    return img


def hero_background(size: tuple[int, int], name: str, dark: bool) -> None:
    w, h = size
    base = gradient(size, COLORS["graphite"] if dark else "#EEF4F8", COLORS["blue_dark"] if dark else "#DDEAF2")
    draw = ImageDraw.Draw(base)
    grid = 80
    line = (255, 255, 255, 22) if dark else (21, 127, 196, 30)
    for x in range(-w, w * 2, grid):
        draw.line((x, 0, x - int(w * 0.35), h), fill=line, width=2)
    for y in range(0, h, grid):
        draw.line((0, y, w, y), fill=line, width=1)
    draw.rounded_rectangle((int(w * 0.08), int(h * 0.16), int(w * 0.50), int(h * 0.78)), radius=24, fill=(255, 255, 255, 28 if dark else 120))
    mark = draw_mark(min(int(h * 0.72), 760), transparent=True)
    base.alpha_composite(mark, (int(w * 0.57), int(h * 0.14)))
    title_font = font(max(46, int(w * 0.052)), True)
    sub_font = font(max(24, int(w * 0.021)), False)
    title = "Технопоиск"
    sub = "Быстрый поиск по документам и данным"
    text_color = COLORS["white"] if dark else COLORS["graphite"]
    draw.text((int(w * 0.11), int(h * 0.30)), title, font=title_font, fill=text_color)
    draw.text((int(w * 0.11), int(h * 0.43)), sub, font=sub_font, fill=COLORS["line"] if dark else COLORS["slate"])
    base.convert("RGB").save(BACKGROUNDS / name)


def open_graph() -> None:
    img = Image.new("RGB", (1200, 630), "#F6F8FA")
    draw = ImageDraw.Draw(img)
    draw.rectangle((0, 0, 1200, 630), fill="#F6F8FA")
    draw.rectangle((0, 0, 1200, 18), fill=COLORS["amber"])
    mark = draw_mark(430, transparent=True)
    img.paste(mark, (705, 88), mark)
    title_font = font(82, True)
    sub_font = font(34, False)
    draw.text((82, 180), "Технопоиск", font=title_font, fill=COLORS["graphite"])
    draw.text((86, 288), "Поиск документов и данных", font=sub_font, fill=COLORS["slate"])
    draw.line((86, 365, 470, 365), fill=COLORS["blue"], width=8)
    img.save(PNG / "web-og-1200x630.png")


def splash() -> None:
    img = Image.new("RGB", (1280, 720), COLORS["graphite"])
    draw = ImageDraw.Draw(img)
    draw.rectangle((0, 0, 1280, 720), fill=COLORS["graphite"])
    mark = draw_mark(360, transparent=True)
    img.paste(mark, (460, 96), mark)
    title_font = font(76, True)
    sub_font = font(30, False)
    draw.text((392, 492), "ТЕХНОПОИСК", font=title_font, fill=COLORS["white"])
    draw.text((422, 586), "ПОИСК ДОКУМЕНТОВ И ДАННЫХ", font=sub_font, fill=COLORS["line"])
    draw.rectangle((0, 704, 1280, 720), fill=COLORS["amber"])
    img.save(PNG / "splash-1280x720.png")


def shortcut(name: str, label: str, accent: str) -> None:
    img = Image.new("RGBA", (512, 512), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle((32, 32, 480, 480), radius=52, fill=COLORS["graphite"])
    draw.rounded_rectangle((58, 58, 454, 454), radius=36, outline=accent, width=14)
    mark = draw_mark(300, transparent=True)
    img.alpha_composite(mark, (106, 82))
    f = font(48, True)
    tw, _ = text_size(draw, label, f)
    draw.rounded_rectangle((118, 384, 394, 442), radius=14, fill=accent)
    draw.text(((512 - tw) // 2, 390), label, font=f, fill=COLORS["white"])
    img.save(SHORTCUTS / f"{name}-512.png")


def write_svg_assets() -> None:
    palette = COLORS
    mark_svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1024 1024" role="img" aria-label="Технопоиск">
  <path d="M210 332h575a28 28 0 0 1 28 28v270H210z" fill="{palette['amber']}"/>
  <path d="M210 268h185a28 28 0 0 1 24 14l34 58H210z" fill="{palette['amber']}"/>
  <path d="M250 356h522v228H250z" fill="none" stroke="{palette['white']}" stroke-width="28" stroke-linejoin="round"/>
  <path d="M360 145h242l53 53v367H360z" fill="{palette['paper']}" stroke="{palette['slate']}" stroke-width="10"/>
  <path d="M602 145v53h53" fill="none" stroke="{palette['slate']}" stroke-width="10"/>
  <path d="M410 245h170M410 300h194M410 355h194M410 410h194M410 465h194" stroke="#81909E" stroke-width="14"/>
  <circle cx="436" cy="236" r="35" fill="{palette['amber_dark']}"/><circle cx="436" cy="236" r="14" fill="{palette['white']}"/>
  <path d="M390 470l64-66 72 31M390 470l-34 70M450 548h190M505 372h55v60h-55z" fill="none" stroke="{palette['graphite_2']}" stroke-width="18" stroke-linecap="round" stroke-linejoin="round"/>
  <circle cx="696" cy="562" r="137" fill="{palette['white']}" stroke="{palette['blue']}" stroke-width="30"/>
  <path d="M790 660l112 112" stroke="{palette['blue']}" stroke-width="54" stroke-linecap="round"/>
  <path d="M636 532a62 62 0 0 1 66-55" fill="none" stroke="{palette['slate']}" stroke-width="18" stroke-linecap="round"/>
</svg>
"""
    logo_svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1600 520" role="img" aria-label="Технопоиск: поиск документов и данных">
  <defs><style>.title{{font:800 126px Arial, sans-serif}}.sub{{font:700 39px Arial, sans-serif;letter-spacing:1px}}</style></defs>
  <use href="#mark" x="52" y="32" width="430" height="430"/>
  <text x="520" y="285" class="title" fill="{palette['amber_dark']}">ТЕХНО</text>
  <text x="875" y="285" class="title" fill="{palette['graphite']}">ПОИСК</text>
  <path d="M520 316h90" stroke="{palette['blue']}" stroke-width="7"/>
  <text x="635" y="328" class="sub" fill="{palette['slate']}">ПОИСК ДОКУМЕНТОВ И ДАННЫХ</text>
  <symbol id="mark" viewBox="0 0 1024 1024">
    <path d="M210 332h575a28 28 0 0 1 28 28v270H210z" fill="{palette['amber']}"/>
    <path d="M210 268h185a28 28 0 0 1 24 14l34 58H210z" fill="{palette['amber']}"/>
    <path d="M250 356h522v228H250z" fill="none" stroke="{palette['white']}" stroke-width="28" stroke-linejoin="round"/>
    <path d="M360 145h242l53 53v367H360z" fill="{palette['paper']}" stroke="{palette['slate']}" stroke-width="10"/>
    <path d="M602 145v53h53" fill="none" stroke="{palette['slate']}" stroke-width="10"/>
    <path d="M410 245h170M410 300h194M410 355h194M410 410h194M410 465h194" stroke="#81909E" stroke-width="14"/>
    <circle cx="436" cy="236" r="35" fill="{palette['amber_dark']}"/><circle cx="436" cy="236" r="14" fill="{palette['white']}"/>
    <path d="M390 470l64-66 72 31M390 470l-34 70M450 548h190M505 372h55v60h-55z" fill="none" stroke="{palette['graphite_2']}" stroke-width="18" stroke-linecap="round" stroke-linejoin="round"/>
    <circle cx="696" cy="562" r="137" fill="{palette['white']}" stroke="{palette['blue']}" stroke-width="30"/>
    <path d="M790 660l112 112" stroke="{palette['blue']}" stroke-width="54" stroke-linecap="round"/>
  </symbol>
</svg>
"""
    (SVG / "mark.svg").write_text(mark_svg, encoding="utf-8")
    (SVG / "logo-horizontal.svg").write_text(logo_svg, encoding="utf-8")


def write_design_strategy() -> None:
    md = f"""# Дизайн-стратегия Технопоиска

## Позиционирование

Приложение выглядит как внутренний рабочий инструмент для поиска строительной, проектной и операционной документации. Визуальный образ соединяет три признака: папка с архивом, документ как источник данных и лупа как главный сценарий поиска. Техника оставлена в виде лаконичного линейного экскаватора, чтобы сохранить связь с исходными примерами и не перегружать маленькие иконки.

## Палитра

- Графит: `{COLORS['graphite']}` — основной текст, тёмные поверхности, контрастные пиктограммы.
- Синий поиска: `{COLORS['blue']}` — лупа, ссылки, активные состояния, web/favicon.
- Янтарный: `{COLORS['amber']}` — папка, строительный акцент, ярлыки быстрых действий.
- Белый: `{COLORS['white']}` и бумажный `{COLORS['paper']}` — документы, светлые фоны, зоны чтения.
- Зелёный: `{COLORS['green']}` — только успешные статусы и подтверждения.

## Логотип и текст

Основная надпись: `ТЕХНОПОИСК`. Подпись: `ПОИСК ДОКУМЕНТОВ И ДАННЫХ`. В исходном варианте с распознанной опечаткой текст заменён на эту форму. Для маленьких размеров используется только знак без подписи.

## Применение

- `assets/brand/ico/app.ico` и корневой `icon.ico` — иконка Windows/exe.
- `assets/brand/ico/favicon.ico` — favicon для web.
- `assets/brand/png/pwa-192.png` и `assets/brand/png/pwa-512.png` — PWA/web manifest.
- `assets/brand/avatars/bot-avatar-512.png` — аватар Telegram-бота.
- `assets/brand/png/web-og-1200x630.png` — preview-картинка для ссылок.
- `assets/brand/backgrounds/*` — hero, splash и фоны экранов.
- `assets/brand/shortcuts/*` — ярлыки быстрых сценариев.

## UI-правила

Фон интерфейса должен оставаться светлым и спокойным, а цветные элементы должны обозначать действие или тип данных. Карточки результатов лучше оставлять белыми с тонкой границей. Янтарный не использовать как фон больших секций; он должен работать как акцент папки, бейджа или состояния. Синий применять для поиска, ссылок и активных кнопок. Для тёмных экранов использовать графитовый фон и белые документы, чтобы знак сохранял читаемость.

## Минимальные размеры

Знак читается с 24 px, но экскаватор в нём становится декоративным. Для 16 px favicon допускается восприятие только как папка + лупа. Полный горизонтальный логотип использовать от 320 px по ширине.
"""
    (ASSETS / "DESIGN_STRATEGY.md").write_text(md, encoding="utf-8")


def copy_sources() -> None:
    candidates = [
        Path("D:/Downloads/gpt-image-1.5-high-fidelity_a_логотип_внутреннего_.png"),
        Path("D:/Downloads/flux-2-max-20251222_b_логотип_внутреннего_.jpeg"),
    ]
    for src in candidates:
        if src.exists():
            shutil.copy2(src, SOURCE / src.name)


def main() -> None:
    ensure_dirs()
    copy_sources()
    write_svg_assets()

    mark = draw_mark(1024, transparent=True)
    badge = draw_mark(1024, transparent=True, badge=True)
    mark.save(PNG / "mark-1024.png")
    badge.save(PNG / "mark-badge-1024.png")
    save_png_variants(mark, "app-icon", [16, 24, 32, 48, 64, 128, 256, 512])
    save_png_variants(badge, "app-badge", [64, 128, 256, 512])
    mark.resize((192, 192), Image.Resampling.LANCZOS).save(PNG / "pwa-192.png")
    mark.resize((512, 512), Image.Resampling.LANCZOS).save(PNG / "pwa-512.png")

    logo_horizontal().save(PNG / "logo-horizontal-1600x520.png")
    logo_stacked().save(PNG / "logo-stacked-1200.png")
    bot_avatar().resize((512, 512), Image.Resampling.LANCZOS).save(AVATARS / "bot-avatar-512.png")
    user_avatar("admin", COLORS["graphite"], "АД").save(AVATARS / "admin-avatar-512.png")
    user_avatar("user", COLORS["blue"], "П").save(AVATARS / "user-avatar-512.png")

    hero_background((1920, 1080), "desktop-dark-1920x1080.png", dark=True)
    hero_background((1600, 900), "web-hero-light-1600x900.png", dark=False)
    hero_background((1080, 1920), "mobile-dark-1080x1920.png", dark=True)
    open_graph()
    splash()
    shortcut("search", "ПОИСК", COLORS["blue"])
    shortcut("index", "ИНДЕКС", COLORS["amber_dark"])
    shortcut("bot", "БОТ", COLORS["green"])

    ico_images = [mark.resize((s, s), Image.Resampling.LANCZOS) for s in [16, 24, 32, 48, 64, 128, 256]]
    ico_images[-1].save(ICO / "app.ico", sizes=[(s, s) for s in [16, 24, 32, 48, 64, 128, 256]], append_images=ico_images[:-1])
    ico_images[2].save(ICO / "favicon.ico", sizes=[(16, 16), (32, 32), (48, 48)], append_images=ico_images[:3])
    if (ROOT / "icon.ico").exists() and not (SOURCE / "icon-legacy.ico").exists():
        shutil.copy2(ROOT / "icon.ico", SOURCE / "icon-legacy.ico")
    shutil.copy2(ICO / "app.ico", ROOT / "icon.ico")

    write_design_strategy()


if __name__ == "__main__":
    main()
