import os
import re
import time
import requests
import json
import logging
import subprocess
from PIL import Image, ImageDraw, ImageFont
import textwrap

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# --- Constants and Configuration ---
WORKER_PUBLIC_URL = os.environ.get("WORKER_PUBLIC_URL")
RAILWAY_API_TOKEN = os.environ.get("RAILWAY_API_TOKEN")
RAILWAY_SERVICE_ID = os.environ.get("RAILWAY_SERVICE_ID")
UPSTASH_REDIS_REST_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

if not all([WORKER_PUBLIC_URL, RAILWAY_API_TOKEN, RAILWAY_SERVICE_ID, UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN]):
    raise ValueError("One or more required environment variables are not set!")

# --- Video Processing Constants ---
COMP_WIDTH = 1080
COMP_HEIGHT = 1920
COMP_SIZE_STR = f"{COMP_WIDTH}x{COMP_HEIGHT}"
BACKGROUND_COLOR = "black"
FPS = 30
MEDIA_Y_OFFSET = 100
CAPTION_V_PADDING = 37
CAPTION_FONT_SIZE = 55
CAPTION_TOP_PADDING_LINES = 0
CAPTION_LINE_SPACING = 12
CAPTION_FONT = "Montserrat-ExtraBold"
CAPTION_TEXT_COLOR = (0, 0, 0)
CAPTION_BG_COLOR = (255, 255, 255)
SILENCE_THRESHOLD_DB = "-50dB"

# --- File Paths ---
DOWNLOAD_PATH = "downloads"
OUTPUT_PATH = "outputs"

# --- Helper Functions (Unchanged) ---
def cleanup_files(file_list):
    for file_path in file_list:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logging.info(f"Cleaned up file: {file_path}")
            except OSError as e:
                logging.error(f"Error deleting file {file_path}: {e}")

def create_directories():
    for path in [DOWNLOAD_PATH, OUTPUT_PATH]:
        if not os.path.exists(path):
            os.makedirs(path)

# --- Railway & Worker Functions (Unchanged) ---
def stop_railway_deployment():
    logging.info("Attempting to stop Railway deployment...")
    api_token, service_id = os.environ.get("RAILWAY_API_TOKEN"), os.environ.get("RAILWAY_SERVICE_ID")
    if not api_token or not service_id:
        logging.warning("RAILWAY variables not set. Skipping stop.")
        return
    graphql_url, headers = "https://backboard.railway.app/graphql/v2", {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
    get_id_query = {"query": "query getLatestDeployment($serviceId: String!) { service(id: $serviceId) { deployments(first: 1) { edges { node { id } } } } }", "variables": {"serviceId": service_id}}
    try:
        response = requests.post(graphql_url, json=get_id_query, headers=headers, timeout=15)
        response.raise_for_status()
        edges = response.json().get('data', {}).get('service', {}).get('deployments', {}).get('edges', [])
        if not edges:
             logging.warning("No active deployments found to stop.")
             return
        deployment_id = edges[0]['node']['id']
    except Exception as e:
        logging.error(f"Failed to get Railway deployment ID: {e}")
        return
    stop_mutation = {"query": "mutation deploymentStop($id: String!) { deploymentStop(id: $id) }", "variables": {"id": deployment_id}}
    try:
        response = requests.post(graphql_url, json=stop_mutation, headers=headers, timeout=15)
        response.raise_for_status()
        logging.info("Successfully sent stop command to Railway.")
    except Exception as e:
        logging.error(f"Failed to send stop command: {e}")

def fetch_job_from_redis():
    url = f"{UPSTASH_REDIS_REST_URL}/rpop/job_queue"
    headers = {"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        result = response.json().get("result")
        return json.loads(result) if result else None
    except Exception as e:
        logging.error(f"Redis fetch failed: {e}")
        return None

def submit_result_to_worker(job_data, video_path):
    url = f"{WORKER_PUBLIC_URL}/submit-result"
    logging.info(f"Submitting result for job {job_data['job_id']}...")
    try:
        with open(video_path, 'rb') as video_file:
            files = {'video': ('final_video.mp4', video_file, 'video/mp4'), 'job_data': (None, json.dumps(job_data), 'application/json')}
            response = requests.post(url, files=files, timeout=300)
            response.raise_for_status()
        logging.info("Successfully submitted result to worker.")
    except Exception as e:
        logging.error(f"Error submitting to worker: {e}")

# --- Core Processing Logic (Unchanged) ---
def download_file_from_url(url, save_path):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        with requests.get(url, stream=True, timeout=60, headers=headers) as r:
            r.raise_for_status()
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): f.write(chunk)
        logging.info(f"Downloaded: {save_path}")
        return save_path
    except Exception as e:
        logging.error(f"Download failed for {url}: {e}")
        return None

def analyze_media_properties(media_path):
    try:
        command = ['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=width,height,duration', '-of', 'json', media_path]
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=30)
        video_data = json.loads(result.stdout)['streams'][0]
        width, height, duration = int(video_data['width']), int(video_data['height']), float(video_data['duration'])
        command_audio_check = ['ffprobe', '-v', 'error', '-select_streams', 'a', '-show_entries', 'stream=codec_type', '-of', 'json', media_path]
        result_audio_check = subprocess.run(command_audio_check, capture_output=True, text=True, timeout=30)
        has_audio_stream = len(json.loads(result_audio_check.stdout).get('streams', [])) > 0
        if not has_audio_stream:
            logging.info(f"Media properties: {width}x{height}, {duration:.2f}s. No audio stream found.")
            return width, height, duration, True
        command_silence = ['ffmpeg', '-i', media_path, '-af', f'silencedetect=noise={SILENCE_THRESHOLD_DB}', '-f', 'null', '-']
        result_silence = subprocess.run(command_silence, capture_output=True, text=True, timeout=60)
        total_silence = 0
        for line in result_silence.stderr.split('\n'):
            if "silencedetect" in line and "silence_duration" in line:
                 duration_match = re.search(r'silence_duration: (\d+\.?\d*)', line)
                 if duration_match:
                     total_silence += float(duration_match.group(1))
        is_silent = total_silence >= (duration - 0.1)
        logging.info(f"Media properties: {width}x{height}, {duration:.2f}s. Detected silence: {total_silence:.2f}s. Is effectively silent: {is_silent}")
        return width, height, duration, is_silent
    except Exception as e:
        logging.error(f"FFprobe/FFmpeg analysis failed: {e}")
        return None, None, None, True

def create_caption_image(text, job_id):
    padded_text = ("\n" * CAPTION_TOP_PADDING_LINES) + text
    font_path = f"{CAPTION_FONT}.ttf"
    if not os.path.exists(font_path): raise FileNotFoundError(f"Font file not found: {font_path}")
    font = ImageFont.truetype(font_path, CAPTION_FONT_SIZE)
    final_lines = [item for line in padded_text.split('\n') for item in textwrap.wrap(line, width=30, break_long_words=True) or ['']]
    wrapped_text = "\n".join(final_lines)
    dummy_draw = ImageDraw.Draw(Image.new('RGB', (0,0)))
    text_bbox = dummy_draw.multiline_textbbox((0, 0), wrapped_text, font=font, align="center", spacing=CAPTION_LINE_SPACING)
    text_height = text_bbox[3] - text_bbox[1]
    rect_height = text_height + (2 * CAPTION_V_PADDING)
    img_height = int(rect_height)
    img = Image.new('RGBA', (COMP_WIDTH, img_height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.rectangle([(0, 0), (COMP_WIDTH, img_height)], fill=CAPTION_BG_COLOR)
    draw.multiline_text((COMP_WIDTH / 2, img_height / 2), wrapped_text, font=font, fill=CAPTION_TEXT_COLOR, anchor="mm", align="center", spacing=CAPTION_LINE_SPACING)
    caption_image_path = os.path.join(OUTPUT_PATH, f"caption_{job_id}.png")
    img.save(caption_image_path)
    return caption_image_path, rect_height

def process_video_job(job_data):
    job_id = job_data['job_id']
    logging.info(f"Starting processing for job_id: {job_id}")
    files_to_clean = []
    try:
        media_path = download_file_from_url(job_data['bg_link'], os.path.join(DOWNLOAD_PATH, f"bg_{job_id}.mp4"))
        bgm_path = download_file_from_url(job_data['bgm_link'], os.path.join(DOWNLOAD_PATH, f"bgm_{job_id}.mp3"))
        if not media_path or not bgm_path: raise ValueError("Media or BGM download failed.")
        files_to_clean.extend([media_path, bgm_path])
        media_w, media_h, final_duration, is_effectively_silent = analyze_media_properties(media_path)
        if not all([media_w is not None, media_h is not None, final_duration is not None]):
            raise ValueError("Could not get media dimensions.")
        caption_image_path, caption_height = create_caption_image(job_data['quote'], job_id)
        files_to_clean.append(caption_image_path)
        output_filepath = os.path.join(OUTPUT_PATH, f"output_{job_id}.mp4")
        scale_ratio = COMP_WIDTH / media_w
        scaled_media_h = int(media_h * scale_ratio)
        media_y_pos = (COMP_HEIGHT / 2 - scaled_media_h / 2) + MEDIA_Y_OFFSET
        caption_y_pos = media_y_pos - caption_height + 1
        command = ['ffmpeg', '-y', '-f', 'lavfi', '-i', f'color=c={BACKGROUND_COLOR}:s={COMP_SIZE_STR}:d={final_duration}', '-i', media_path, '-i', caption_image_path]
        filter_parts = [f"[1:v]scale={COMP_WIDTH}:-1,setpts=PTS-STARTPTS[scaled_media]", f"[0:v][scaled_media]overlay=(W-w)/2:{media_y_pos}[bg_with_media]", f"[bg_with_media][2:v]overlay=(W-w)/2:{caption_y_pos}[final_v]"]
        if not is_effectively_silent:
            logging.info("Source video has detectable audio. Using original audio track.")
            filter_parts.append(f"[1:a]asetpts=PTS-STARTPTS[final_a]")
            map_args = ['-map', '[final_v]', '-map', '[final_a]']
        else:
            logging.info("Source video is silent or has no audio. Applying background music.")
            command.extend(['-i', bgm_path])
            filter_parts.append(f"[3:a]asetpts=PTS-STARTPTS[final_a]")
            map_args = ['-map', '[final_v]', '-map', '[final_a]']
        filter_complex = ";".join(filter_parts)
        command.extend(['-filter_complex', filter_complex, *map_args, '-c:v', 'libx264', '-preset', 'superfast', '-tune', 'zerolatency', '-c:a', 'aac', '-b:a', '192k', '-r', str(FPS), '-pix_fmt', 'yuv420p', '-t', str(final_duration), '-shortest', output_filepath])
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logging.error(f"FFMPEG STDERR: {result.stderr}")
            raise subprocess.CalledProcessError(result.returncode, command, stderr=result.stderr)
        logging.info(f"FFmpeg processing finished for job {job_id}.")
        files_to_clean.append(output_filepath)
        submit_result_to_worker(job_data, output_filepath)
    except Exception as e:
        error_snippet = str(e)[-1000:]
        logging.error(f"Failed to process job {job_id}: {error_snippet}", exc_info=True)
    finally:
        logging.info(f"Cleaning up files for job {job_id}.")
        cleanup_files(files_to_clean)

# --- Main Bot Loop (MODIFIED AS REQUESTED) ---
if __name__ == '__main__':
    logging.info("Starting Python Job Processor...")
    create_directories()
    
    # Fetch a single job from the queue. No polling or timeout.
    job = fetch_job_from_redis()
    
    if job:
        # If a job is found, process it.
        logging.info("Job found. Processing...")
        process_video_job(job)
    else:
        # If no job is found, just log it.
        logging.info("No job found in queue.")
    
    # In both cases (job processed or no job found), immediately request shutdown.
    logging.info("Task complete. Requesting shutdown.")
    stop_railway_deployment()
    logging.info("Processor has finished its work and is exiting.")
