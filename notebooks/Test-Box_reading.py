import cv2
import fitz  # PyMuPDF
import os
from dotenv import load_dotenv
from pathlib import Path
import numpy as np
import pytesseract  # optional, for text extraction

# ---------------------------------------------------------------------
# Load environment variables and PDF file
# ---------------------------------------------------------------------
load_dotenv()
WS = os.getenv("DATA_FILES")

file = Path(WS) / "MARCH_2025.pdf"
doc = fitz.open(file)

# ---------------------------------------------------------------------
# Loop through all pages
# ---------------------------------------------------------------------
for num in range(doc.page_count):
    # Convert current PDF page → RGB numpy array
    page = doc.load_page(num)
    pix = page.get_pixmap(dpi=300)
    img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, 3).copy()
    img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)

    # -----------------------------------------------------------------
    # Preprocessing
    # -----------------------------------------------------------------
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)

    # Threshold (invert to detect light boxes on dark text)
    _, thresh = cv2.threshold(blur, 200, 255, cv2.THRESH_BINARY_INV)

    # Optional: Morphological closing to smooth out box edges
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (15, 3))
    closed = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel)

    # -----------------------------------------------------------------
    # Find and draw contours (possible boxes)
    # -----------------------------------------------------------------
    contours, _ = cv2.findContours(closed, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    print(f"\n🔍 Page {num+1}: {len(contours)} contours found")

    for c in contours:
        x, y, w, h = cv2.boundingRect(c)
        area = w * h
        aspect_ratio = w / h

        # Filter for rectangular boxes like 'Portfolio Cash'
        if area > 5000 and 3 < aspect_ratio < 20:
            # Draw detected rectangle
            cv2.rectangle(img, (x, y), (x + w, y + h), (0, 255, 0), 3)

            # Extract and OCR text inside the box
            roi = img[y:y+h, x:x+w]
            text = pytesseract.image_to_string(roi).strip()
            print(f"📦 Detected box at ({x},{y},{w},{h}) → '{text}'")

    # -----------------------------------------------------------------
    # Show result for current page
    # -----------------------------------------------------------------
    cv2.imshow(f"Detected Boxes - Page {num+1}", img)
    cv2.waitKey(0)

cv2.destroyAllWindows()
