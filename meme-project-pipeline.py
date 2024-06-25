import tqdm
from os import scandir
import random
from io import BytesIO
import json
import sqlite3
from copy import deepcopy

from PIL import Image, ImageFont, ImageDraw, ImageEnhance

def initdb():

    con = sqlite3.connect('meme-project.db')
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS memes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin TEXT,
            path TEXT UNIQUE,
            font TEXT,
            font_size INTEGER,
            stroke_width INTEGER,
            fill_color TEXT,
            stroke_color TEXT,
            top TEXT,
            bottom TEXT,
            angle INTEGER,
            jpg_quality INTEGER,
            brightness REAL,
            contrast REAL,
            sharpness REAL,
            color REAL
        );
        """
    )
    con.commit()
    cur.close()
    return con

class Meme:
    def __init__(self, image_path) -> None:
        self.im = Image.open(image_path)
        self.info = {'origin':image_path}

    def _get_wrapped_text(self, text: str, font,
                         line_length: float, height: float, drawer:ImageDraw.ImageDraw,force=False):
        lines = ['']
        for word in text.split():
            line = f'{lines[-1]} {word}'.strip()
            if font.getlength(line) <= line_length:
                lines[-1] = line
            else:
                lines.append(word)
        if not force and len(lines) > 3: return False
        text = '\n'.join(lines)
        _, top, _, bottom = drawer.multiline_textbbox((0,0), text, anchor='la', font=font)
        if not force and bottom - top > height: return False
        return text

    def _get_font_size(self, text, line_length, height, drawer, font_path):
        font_size = 40
        font = ImageFont.truetype(font_path, size=font_size)
        while font_size > 20:
            stroke_size = 3*font.size/20-2
            if self._get_wrapped_text(text, font, line_length, height, drawer):
                break
            font_size -= 1
            font = ImageFont.truetype(font_path, size=font_size)
        else:
            stroke_size = 3*font.size/20-2
        return (font, round(stroke_size))

    def add_text(self, top, bottom, margin_h=10, margin_v=10, fill_color = (255, 255, 255), stroke_color = (0, 0, 0)):
        self.info['bottom'] = bottom
        width, height = self.im.size
        font_path = self._randomfont()
        line_length = width-2*margin_h
        max_height = height/3
        drawer = ImageDraw.Draw(self.im)
        font, stroke_width =  self._get_font_size(max(top, bottom, key=lambda x: len(x)), line_length, max_height, drawer, font_path)
        self.info['top'] = top
        self.info['font'] = ' '.join(font.getname())
        self.info['font_size'] = font.size
        self.info['stroke_width'] = stroke_width
        self.info['fill_color'] = str(fill_color)
        self.info['stroke_color'] = str(stroke_color)
        top = self._get_wrapped_text(top, font, line_length, max_height, drawer, force=True)
        bottom = self._get_wrapped_text(bottom, font, line_length, max_height, drawer, force=True)
        drawer.text((width/2, margin_v), top, align='center', anchor='ma', font=font, fill=fill_color, stroke_width=stroke_width, stroke_fill=stroke_color)
        drawer.text((width/2, height-margin_v), bottom, align='center', anchor='md', font=font, fill=fill_color, stroke_width=stroke_width, stroke_fill=stroke_color)

    def jpg(self, con:sqlite3.Connection, path='.'):
        quality = random.randint(0,99)
        self.info['jpg_quality'] = quality
        self.save(con, path, quality=quality)

    def rotate(self):
        a = random.randint(-5,5)
        self.info['angle'] = a
        self.im = self.im.rotate(a)

    def enhance(self, filter, name):
        filter = filter(self.im)
        b = 1 + random.randint(-3,3)/10
        self.info[name] = b
        self.im = filter.enhance(b)

    def save(self, con:sqlite3.Connection, path='.', quality=100):
        buffer = BytesIO()
        self.im.save(buffer, format='jpeg', quality=quality)
        h = f'{path}/{hash(self)}.jpg'
        with open(h, 'wb') as f:
            f.write(buffer.getbuffer())
        self.info['path'] = h
        self._insert_info(con)

    def _insert_info(self, con:sqlite3.Connection):
        keys = ', '.join(self.info.keys())
        insert = f"INSERT INTO memes({keys}) VALUES({', '.join(['?']*len(self.info))})"
        cur = con.cursor()
        cur.execute(insert, tuple(self.info.values()))
        con.commit()
        cur.close()

    def _randomfont(self):
        ttf = list(map(lambda x: f"TTF/{x.name}", scandir("/usr/share/fonts/TTF/")))
        otf = list(map(lambda x: f"OTF/{x.name}", scandir("/usr/share/fonts/OTF/")))
        return f"/usr/share/fonts/{random.choice(ttf + otf)}"

    def __hash__(self) -> int:
        return hash(tuple(self.info.items()))

def pipeline(image_path, con, path):
    json_path = f'{image_path.split('.')[0]}.json'
    meme = Meme(image_path)
    effects_pipline(meme, path, con)
    with open(json_path) as f:
        d = json.load(f)

    texts = d[:3]

    for text in texts:
        meme_t = deepcopy(meme)
        meme_t.add_text(text[0], text[1])
        meme_t.save(con, path)
        effects_pipline(meme_t, path, con)

def effects_pipline(meme:Meme, path:str, con:sqlite3.Connection):
    meme_t = deepcopy(meme)
    meme_t.enhance(ImageEnhance.Brightness, 'brightness')
    meme_t.save(con, path)

    meme_t = deepcopy(meme)
    meme_t.enhance(ImageEnhance.Contrast, 'contrast')
    meme_t.save(con, path)

    meme_t = deepcopy(meme)
    meme_t.enhance(ImageEnhance.Color, 'color')
    meme_t.save(con, path)

    meme_t = deepcopy(meme)
    meme_t.enhance(ImageEnhance.Sharpness, 'sharpness')
    meme_t.save(con, path)

    meme_t = deepcopy(meme)
    meme_t.rotate()
    meme_t.save(con, path)

    meme_t = deepcopy(meme)
    meme_t.jpg(con, path)

    meme_t = deepcopy(meme)
    meme_t.rotate()
    meme_t.enhance(ImageEnhance.Brightness, 'brightness')
    meme_t.enhance(ImageEnhance.Contrast, 'contrast')
    meme_t.enhance(ImageEnhance.Color, 'color')
    meme_t.enhance(ImageEnhance.Sharpness, 'sharpness')
    meme_t.jpg(con, path)



def main():

    con = initdb()

    for path in tqdm.tqdm(scandir('meme-project-raw-dataset/'), total=6000):
        if 'jpg' != path.name.split('.')[1]:
            continue
        pipeline(path.path, con, 'meme-project-pipeline-result/')
    con.close()

if __name__ == "__main__":
    main()
