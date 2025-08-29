# pyright: reportMissingImports=false, reportMissingTypeStubs=false
from datetime import datetime
import time
import os
import io
import json as _json
from typing import Any, Dict, List, Optional
import uuid
import logging
from flask import Flask, request, jsonify, make_response, redirect
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import stripe  # type: ignore
from flask_cors import CORS  # type: ignore
from bson import ObjectId
from OpenSSL import crypto  # type: ignore
import ssl
import socket
import requests  # type: ignore
from pypdf import PdfReader  # type: ignore
from openai import OpenAI

app = Flask(__name__)
CORS(app)

# SSL Certificate Generation
def generate_self_signed_cert(cert_file, key_file):
    # Check if certificate files already exist
    if os.path.exists(cert_file) and os.path.exists(key_file):
        return
    
    # Create a key pair
    k = crypto.PKey()
    k.generate_key(crypto.TYPE_RSA, 2048)
    
    # Create a self-signed cert
    cert = crypto.X509()
    cert.get_subject().C = "US"
    cert.get_subject().ST = "Massachusetts"
    cert.get_subject().L = "Boston"
    cert.get_subject().O = "Fineas.ai"
    cert.get_subject().OU = "Fineas.ai"
    cert.get_subject().CN = socket.gethostname()
    cert.set_serial_number(1000)
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(10*365*24*60*60)  # Valid for 10 years
    cert.set_issuer(cert.get_subject())
    cert.set_pubkey(k)
    cert.sign(k, 'sha256')
    
    # Save certificate
    with open(cert_file, "wb") as f:
        f.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
    
    # Save private key
    with open(key_file, "wb") as f:
        f.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, k))

# Generate SSL certificates if they don't exist
CERT_FILE = "server.crt"
KEY_FILE = "server.key"
generate_self_signed_cert(CERT_FILE, KEY_FILE)

MONGO_PASS = str(os.getenv("MONGO_DB_LOGGER_PASSWORD")) or ""
REDIRECT_DOMAIN = str(os.getenv("REDIRECT_DOMAIN")) or ""
uri = "mongodb+srv://kobenaidun:"+MONGO_PASS+"@cluster0.z9znpv9.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
client: MongoClient = MongoClient(uri, server_api=ServerApi('1'))
db = client['User']  # Your MongoDB database
userlist = db['UserInformation']  # Your MongoDB collection name
course_db = client['Courses']
user_params_collection = course_db['UserParams']
course_data_collection = course_db['coursedata']
integrations_db = client['Integrations']
portfolio_list = integrations_db['Portfolios']
schwab_tokens_list = integrations_db['SchwabTokens']

stripe.api_key = os.getenv("STRIPE_SECRET_KEY")  # Set your Stripe API key

# Initialize OpenAI client lazily inside endpoint to avoid import errors if key missing
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or ""
# Course content rewrite configuration (to avoid timeouts/OOM). Defaults disable rewrites.
COURSE_REWRITE_MAX = int(os.getenv("COURSE_REWRITE_MAX", "1"))
try:
    COURSE_REWRITE_TIMEOUT_S = float(os.getenv("COURSE_REWRITE_TIMEOUT_S", "15"))
except Exception:
    COURSE_REWRITE_TIMEOUT_S = 15.0

# Request-level time budget to avoid Gunicorn worker timeouts (Gunicorn default ~30s)
try:
    COURSE_REQUEST_TIME_BUDGET_S = float(os.getenv("COURSE_REQUEST_TIME_BUDGET_S", "18"))
except Exception:
    COURSE_REQUEST_TIME_BUDGET_S = 18.0

def _extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    text_parts: List[str] = []
    for page in reader.pages:
        try:
            text_parts.append(page.extract_text() or "")
        except Exception:
            # Skip pages that fail to extract
            continue
    return "\n".join([t for t in text_parts if t])

def _chunk_text(text: str, max_chars: int = 2500, overlap: int = 150) -> List[str]:
    if max_chars <= 0:
        return [text]
    chunks: List[str] = []
    start = 0
    n = len(text)
    while start < n:
        end = min(start + max_chars, n)
        # Try to break on a paragraph or sentence boundary
        slice_text = text[start:end]
        last_break = max(slice_text.rfind("\n\n"), slice_text.rfind(". "))
        if last_break > 0 and (start + last_break) - start > max_chars * 0.5:
            end = start + last_break + 1
            slice_text = text[start:end]
        chunks.append(slice_text.strip())
        if end >= n:
            break
        start = max(0, end - overlap)
    return [c for c in chunks if c]

def _sanitize_json_response(s: str) -> str:
    # Extract JSON between first { and last }
    if "```" in s:
        # Remove fenced code blocks if present
        s = s.replace("```json", "").replace("```", "")
    start = s.find("{")
    end = s.rfind("}")
    if start != -1 and end != -1 and end > start:
        return s[start:end+1]
    return s

def _count_words(text: str) -> int:
    try:
        return len([w for w in (text or "").split() if w.strip()])
    except Exception:
        return 0

def _trim_to_word_limit(text: str, max_words: int) -> str:
    if not isinstance(text, str):
        return text
    words = [w for w in text.split() if w.strip()]
    if len(words) <= max_words:
        return text
    trimmed = " ".join(words[:max_words])
    return trimmed.strip()

def _to_jsonable(obj: Any) -> Any:
    """Recursively convert Mongo/ObjectId and datetime values to JSON-safe types."""
    try:
        if isinstance(obj, ObjectId):
            return str(obj)
    except Exception:
        pass
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(v) for v in obj]
    return obj

@app.route('/upgrade_membership', methods=['POST'])
def upgrade_membership():
    data = request.json
    id_hash = data.get('id_hash')

    if id_hash is None:
        return make_response(jsonify({'error': 'You must be logged in'}), 400)


    # Retrieve or create a user document
    user = userlist.find_one({'id_hash': id_hash})
    if not user:
        user = {
            'id_hash': id_hash,
            'credits': 10,
            'ismember': False
        }
        user['_id'] = userlist.insert_one(user).inserted_id
        user['_id'] = str(user['_id'])

    # Create a new Stripe customer
    customer = stripe.Customer.create()
    stripe_customer_id = customer.id

    # Log the created Stripe customer ID
    print(f"Created Stripe customer ID: {stripe_customer_id}")

    # Save the Stripe customer ID in the user's MongoDB document
    update_result = userlist.update_one({'id_hash': id_hash}, {'$set': {'stripe_customer_id': stripe_customer_id}})

    # Check if the update was successful
    if update_result.modified_count == 1:
        print(f"Successfully saved stripe_customer_id for user with id_hash {id_hash}")
    else:
        print(f"Failed to save stripe_customer_id for user with id_hash {id_hash}. Update result: {update_result.raw_result}")

    # Verify the update by fetching the document again (optional, for debugging)
    user = userlist.find_one({'id_hash': id_hash})
    print(f"User document after update: {user}")

    # Before proceeding with creating a checkout session, check if stripe_customer_id is valid
    if not stripe_customer_id or stripe_customer_id.strip() == "":
        print(f"Invalid stripe_customer_id for user with id_hash {id_hash}: '{stripe_customer_id}'")
        return make_response(jsonify({'error': 'Failed to create or retrieve a valid Stripe customer ID'}), 500)


    # Proceed with creating a checkout session
    # Now, include the customer ID in the session creation
    try:
        price_id = "price_1ROTnyCajx6ndCSa2mc02hcN"  # Replace with your price ID

        checkout_session = stripe.checkout.Session.create(
            customer=stripe_customer_id,
            line_items=[
                {
                    'price': price_id,
                    'quantity': 1,

                },
            ],
            metadata={
                'id_hash': id_hash,
            },
            mode='subscription',
            success_url=REDIRECT_DOMAIN +
            '?success=true&session_id={CHECKOUT_SESSION_ID}',
            cancel_url=REDIRECT_DOMAIN + '?canceled=true',
        )
        # Prepare user information for JSON response (excluding sensitive fields like _id)
        user_info = {
            "id_hash": user['id_hash'],
            "credits": user.get('credits', 0),
            "ismember": user.get('ismember', False),
            "stripe_customer_id": stripe_customer_id
        }

        return jsonify({'checkout_session_id': checkout_session.id, 'user': user_info, 'url' : checkout_session.url}, 200)

    except Exception as e:
        print(e)
        return str(e), 500

@app.route('/session-status', methods=['GET'])
def session_status():
  session = stripe.checkout.Session.retrieve(request.args.get('session_id'))
  return jsonify(status=session.status, customer_email=session.customer_details.email, metadata=session.metadata)

from flask import request, jsonify
from datetime import datetime

@app.route('/get-user-info', methods=['GET'])
def get_user_info():
    id_hash = request.args.get('id_hash')
    email = request.args.get('email')

    if not id_hash:
        return jsonify({'error': 'ID hash is required as a query parameter'}), 400

    user = userlist.find_one({'id_hash': id_hash}, {'_id': 0})

    if user:
        if email and 'email' not in user:
            now = datetime.utcnow()
            user['email'] = email
            user['signup_date'] = now
            userlist.update_one({'id_hash': id_hash}, {'$set': {'email': email, 'signup_date': now}})
    else:
        now = datetime.utcnow()
        user = {'id_hash': id_hash, 'credits': 10, 'ismember': False}
        if email:
            user['email'] = email
            user['signup_date'] = now
        userlist.insert_one(user)

    # serialize response safely (ObjectId, datetime, etc.)
    safe_user = _to_jsonable(user)
    return jsonify({'user': safe_user}), 200

@app.route('/update-profile-picture', methods=['POST'])
def update_profile_picture():
    data = request.json or {}
    id_hash = data.get('id_hash')
    picture_url = data.get('picture_url')

    if not id_hash or not picture_url:
        return jsonify({'error': 'id_hash and picture_url are required'}), 400

    userlist.update_one({'id_hash': id_hash}, {'$set': {'picture_url': picture_url}})
    return jsonify({'success': True}), 200

@app.route('/cancel-subscription', methods=['POST'])
def cancel_subscription():
    data = request.json
    stripe_customer_id = data.get('stripe_customer_id')

    if not stripe_customer_id:
        return make_response(jsonify({'error': 'Stripe customer ID is required'}), 400)

    try:
        # List all active subscriptions for the customer
        subscriptions = stripe.Subscription.list(customer=stripe_customer_id, status='active')
        
        # Cancel all active subscriptions
        for sub in subscriptions.auto_paging_iter():
            stripe.Subscription.delete(sub.id)

        # After successfully canceling subscriptions, update the user in MongoDB
        update_result = userlist.update_one(
            {'stripe_customer_id': stripe_customer_id},
            {'$set': {'ismember': False, 'credits': 3}}
        )

        # Check if the user document was found and updated
        if update_result.matched_count == 0:
            # No user found with that Stripe customer ID
            return jsonify({'warning': 'User not found in database, but subscriptions were canceled'}), 200
        elif update_result.modified_count == 0:
            # Document found but not modified - could indicate the values were already set
            return jsonify({'info': 'User found, but document was not modified'}), 200

        return jsonify({'success': True, 'message': 'All subscriptions canceled and user updated in database'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    

@app.route('/enforce-credits', methods=['POST'])
def enforce_credits():
    data = request.json
    id_hash = data.get('id_hash')
    
    if not id_hash:
        return make_response(jsonify({'error': 'ID hash is required'}), 400)
    
    user = userlist.find_one({'id_hash': id_hash})

    if not user:
        # User not found - this should be an error since user creation happens in get-user-info
        return make_response(jsonify({'error': 'User not found. Please ensure user exists before enforcing credits.'}), 404)

    # User found, check credits
    if user['credits'] > 0:
        # Decrement credits
        new_credits = user['credits'] - 1
        userlist.update_one({'id_hash': id_hash}, {'$set': {'credits': new_credits}})
        user['credits'] = new_credits  # Update local copy for response
    elif not user.get('ismember', False):
        # No credits left and not a member
        return make_response(jsonify({'error': 'Ran out of credits'}), 402)  # 402 Payment Required
    
    # Return the document, but without MongoDB's internal ID
    user.pop('_id', None)
    return jsonify(user)

@app.route('/savechat', methods=['POST'])
def save_chat():
    data = request.json
    chat_name = data.get('chatname')
    id_hash = data.get('id_hash')
    chat_history = data.get('chat_history')

    if not chat_name or not id_hash or not chat_history:
        return jsonify({"error": "Chat name, ID hash, and chat history are required"}), 400

    # Assuming a separate collection for chats
    chat_history_collection = db['chats']
    chat_name_collection = db['chatnames']

    # Check for existing chat with the same name for the user
    existing_chat = chat_history_collection.find_one({"chatname": chat_name, "id_hash": id_hash})
    if existing_chat:
        chat_history_collection.update_one({"chatname": chat_name, "id_hash": id_hash}, {"$set": {"chat_history": chat_history}})
        chat_names = chat_name_collection.find({"id_hash": id_hash}, {'_id': 0})
        return jsonify({"message": "Chat updated successfully", "chat_names": [chat['chatname'] for chat in chat_names]})

    # Save the new chat history
    chat_document = {
        "chatname": chat_name,
        "id_hash": id_hash,
        "chat_history": chat_history
    }
    chat_history_collection.insert_one(chat_document)
    chat_name_collection.insert_one({"chatname": chat_name, "id_hash": id_hash})
    chat_names = chat_name_collection.find({"id_hash": id_hash}, {'_id': 0})
    return jsonify({"message": "Chat saved successfully", "chat_names": [chat['chatname'] for chat in chat_names]})

@app.route('/loadchat', methods=['POST'])
def load_chat():
    data = request.json
    chat_name = data.get('chatname')
    id_hash = data.get('id_hash')

    # Use the same collection for chats as in savechat
    chats_collection = db['chats']

    chat = chats_collection.find_one({"chatname": chat_name, "id_hash": id_hash}, {'_id': 0})
    if chat:
        return jsonify(chat)
    else:
        return jsonify({"error": "Chat not found"}), 404
    

@app.route('/delete-chats', methods=['GET'])
def delete_chat():
    chat_name = request.args.get('chatname')
    id_hash = request.args.get('id_hash')

    if not chat_name or not id_hash:
        return jsonify({"error": "Chat name and ID hash are required"}), 400

    # Use the same collection for chats as in savechat
    chats_collection = db['chats']
    chat_name_collection = db['chatnames']

    # Delete the chat history
    chats_collection.delete_one({"chatname": chat_name, "id_hash": id_hash})
    chat_name_collection.delete_one({"chatname": chat_name, "id_hash": id_hash})

    chat_names = chat_name_collection.find({"id_hash": id_hash}, {'_id': 0})
    return jsonify({"message": "Chat deleted successfully", "chat_names": [chat['chatname'] for chat in chat_names]})

@app.route('/get-chat-names', methods=['GET'])
def get_chat_names():
    id_hash = request.args.get('id_hash')

    if not id_hash:
        return jsonify({"error": "ID hash is required"}), 400

    chat_name_collection = db['chatnames']
    chat_names = chat_name_collection.find({"id_hash": id_hash}, {'_id': 0})
    return jsonify([chat['chatname'] for chat in chat_names])

@app.route('/course-create', methods=['POST'])
def course_create():
    """
    Create a course from a PDF and a prompt.
    Accepts either multipart/form-data with 'pdf' file and 'prompt', or JSON with 'pdf_url' and 'prompt'.
    Optional: 'items_per_module' (default 5).

    Returns a JSON adhering to CourseStruct:
    {
      "title": str,
      "description": str,
      "modules": [
        { "title": str, "pdfUrl": str?, "contentList": [
            {"title": str, "content": str, "pdfUrl"?: str, "moduleTitle"?: str, "mediaUrl"?: str, "mediaType"?: "image"|"pdf"}, ...
        ] }
      ]
    }
    """
    try:
        items_per_module_default = 5
        trace_id = str(uuid.uuid4())
        app.logger.info(f"[course-create] trace_id={trace_id} incoming request content_type={request.content_type}")

        pdf_bytes: Optional[bytes] = None
        prompt: str = ""
        pdf_url: Optional[str] = None
        items_per_module: int = items_per_module_default
        cohort_passwords: Dict[str, str] = {}

        if request.content_type and 'multipart/form-data' in request.content_type:
            uploaded = request.files.get('pdf')
            if not uploaded:
                app.logger.warning(f"[course-create] trace_id={trace_id} missing file field 'pdf'")
                return make_response(jsonify({'error': 'PDF file field "pdf" is required', 'trace_id': trace_id}), 400)
            pdf_bytes = uploaded.read()
            prompt = request.form.get('prompt', '')
            items_per_module = int(request.form.get('items_per_module', items_per_module_default))
            # Optional cohort_passwords mapping (JSON string)
            cp_raw = request.form.get('cohort_passwords')
            if cp_raw:
                try:
                    parsed = _json.loads(cp_raw)
                    if isinstance(parsed, dict):
                        # sanitize: only keep non-empty trimmed keys, coerce values to strings
                        cohort_passwords = {str(k).strip(): str(v) if v is not None else '' for k, v in parsed.items() if str(k).strip()}
                        app.logger.info(f"[course-create] trace_id={trace_id} cohort_passwords_keys={list(cohort_passwords.keys())}")
                    else:
                        app.logger.warning(f"[course-create] trace_id={trace_id} cohort_passwords not a dict; type={type(parsed)}")
                except Exception as e:
                    app.logger.warning(f"[course-create] trace_id={trace_id} cohort_passwords parse error: {str(e)}")
            app.logger.info(f"[course-create] trace_id={trace_id} mode=file filename={getattr(uploaded, 'filename', '')} size_bytes={len(pdf_bytes)} items_per_module={items_per_module}")
        else:
            data = request.json or {}
            pdf_url = data.get('pdf_url')
            prompt = data.get('prompt', '')
            items_per_module = int(data.get('items_per_module', items_per_module_default))
            # Optional cohort_passwords mapping (dict or JSON string)
            cp = data.get('cohort_passwords')
            try:
                if isinstance(cp, str) and cp:
                    parsed = _json.loads(cp)
                    if isinstance(parsed, dict):
                        cohort_passwords = {str(k).strip(): str(v) if v is not None else '' for k, v in parsed.items() if str(k).strip()}
                elif isinstance(cp, dict):
                    cohort_passwords = {str(k).strip(): str(v) if v is not None else '' for k, v in cp.items() if str(k).strip()}
                if cohort_passwords:
                    app.logger.info(f"[course-create] trace_id={trace_id} cohort_passwords_keys={list(cohort_passwords.keys())}")
            except Exception as e:
                app.logger.warning(f"[course-create] trace_id={trace_id} cohort_passwords parse error (json mode): {str(e)}")
            if not pdf_url:
                app.logger.warning(f"[course-create] trace_id={trace_id} missing pdf_url in JSON body")
                return make_response(jsonify({'error': 'pdf_url is required in JSON body when not uploading a file', 'trace_id': trace_id}), 400)
            try:
                app.logger.info(f"[course-create] trace_id={trace_id} downloading pdf_url={pdf_url}")
                resp = requests.get(pdf_url, timeout=60)
                resp.raise_for_status()
                pdf_bytes = resp.content
                app.logger.info(f"[course-create] trace_id={trace_id} downloaded size_bytes={len(pdf_bytes)} status_code={resp.status_code}")
            except Exception as e:
                app.logger.error(f"[course-create] trace_id={trace_id} download error: {str(e)}", exc_info=True)
                return make_response(jsonify({'error': f'Failed to download PDF from url: {str(e)}', 'trace_id': trace_id}), 400)

        if not prompt:
            app.logger.warning(f"[course-create] trace_id={trace_id} missing prompt")
            return make_response(jsonify({'error': 'prompt is required', 'trace_id': trace_id}), 400)
        if not pdf_bytes:
            app.logger.error(f"[course-create] trace_id={trace_id} pdf_bytes empty")
            return make_response(jsonify({'error': 'Failed to obtain PDF bytes', 'trace_id': trace_id}), 400)

        # Extract and chunk text
        request_start_time = time.time()
        try:
            try:
                _reader = PdfReader(io.BytesIO(pdf_bytes))
                page_count = len(_reader.pages)
            except Exception:
                page_count = None
            extracted_text = _extract_text_from_pdf_bytes(pdf_bytes)
            app.logger.info(f"[course-create] trace_id={trace_id} extracted_text_len={len(extracted_text)} page_count={page_count}")
        except Exception as e:
            app.logger.error(f"[course-create] trace_id={trace_id} extraction error: {str(e)}", exc_info=True)
            return make_response(jsonify({'error': 'Unable to extract text from PDF', 'trace_id': trace_id}), 400)
        if not extracted_text:
            return make_response(jsonify({'error': 'Unable to extract text from PDF', 'trace_id': trace_id}), 400)

        chunks = _chunk_text(extracted_text, max_chars=3000, overlap=200)
        if chunks:
            first_len = len(chunks[0])
            last_len = len(chunks[-1])
        else:
            first_len = 0
            last_len = 0
        app.logger.info(f"[course-create] trace_id={trace_id} chunks_count={len(chunks)} first_chunk_len={first_len} last_chunk_len={last_len}")
        # Prepare a compressed corpus to stay within context limits
        # Concatenate first N chunks if too many
        max_chunks = 4
        selected_chunks = chunks[:max_chunks]
        corpus = "\n\n".join([f"[Chunk {i+1}]\n" + c for i, c in enumerate(selected_chunks)])
        # Limit corpus length hard cap
        if len(corpus) > 20000:
            corpus = corpus[:20000]
        app.logger.info(f"[course-create] trace_id={trace_id} selected_chunks={len(selected_chunks)} corpus_len={len(corpus)}")

        if not OPENAI_API_KEY:
            app.logger.error(f"[course-create] trace_id={trace_id} missing OPENAI_API_KEY")
            return make_response(jsonify({'error': 'OPENAI_API_KEY not configured', 'trace_id': trace_id}), 500)

        client = OpenAI(api_key=OPENAI_API_KEY)

        system_instructions = (
            "You are an expert instructional designer. Given a source corpus and a user prompt, "
            "produce a high-quality CourseStruct JSON. Each module must include a contentList with exactly "
            f"{items_per_module} items by default unless otherwise specified. Return strictly valid JSON only."
        )

        user_instructions = {
            "task": "Create a course structure JSON (CourseStruct)",
            "prompt": prompt,
            "items_per_module": items_per_module,
            "schema": {
                "title": "string",
                "description": "string",
                "modules": [
                    {
                        "title": "string",
                        "pdfUrl": "string?",
                        "contentList": [
                            {
                                "title": "string",
                                "content": "string",
                                "pdfUrl": "string?",
                                "moduleTitle": "string?",
                                "mediaUrl": "string?",
                                "mediaType": "image|pdf?"
                            }
                        ]
                    }
                ]
            },
            "source_corpus": corpus[:15000],
            "notes": [
                "Use the source_corpus to ground the content.",
                "Ensure modules are coherent and sequential.",
                "Use concise, skimmable, beginner-friendly language that assumes no prior knowledge.",
                "For each contentList item, write a clear mini-lesson of 300-500 words that explains the topic at hand in simple terms, includes definitions, everyday analogies, and a short step-by-step where relevant.",
                "Set each module.contentList length to items_per_module by default.",
                "If a pdf_url was provided, include it as module.pdfUrl and contentList[i].pdfUrl where relevant.",
                "Do not include any text outside of the JSON object."
            ]
        }

        model_name = os.getenv("OPENAI_MODEL", "gpt-4o")
        initial_timeout = float(os.getenv("COURSE_INITIAL_TIMEOUT_S", str(max(5.0, min(COURSE_REWRITE_TIMEOUT_S, COURSE_REQUEST_TIME_BUDGET_S)))))
        app.logger.info(f"[course-create] trace_id={trace_id} invoking LLM model={model_name} items_per_module={items_per_module} initial_timeout_s={initial_timeout}")
        completion = client.chat.completions.create(
            model=model_name,
            temperature=0.4,
            messages=[
                {"role": "system", "content": system_instructions},
                {"role": "user", "content": _json.dumps(user_instructions)}
            ],
            timeout=initial_timeout
        )

        content = completion.choices[0].message.content if completion and completion.choices else ""
        app.logger.info(f"[course-create] trace_id={trace_id} llm_response_len={len(content) if content else 0}")
        if not content:
            app.logger.error(f"[course-create] trace_id={trace_id} empty LLM response")
            return make_response(jsonify({'error': 'LLM returned empty response', 'trace_id': trace_id}), 502)

        try:
            raw_json = _sanitize_json_response(content)
            course_obj: Dict[str, Any] = _json.loads(raw_json)
            modules_count = len(course_obj.get('modules', []) or []) if isinstance(course_obj, dict) else 0
            app.logger.info(f"[course-create] trace_id={trace_id} parsed_json keys={list(course_obj.keys()) if isinstance(course_obj, dict) else 'n/a'} modules_count={modules_count}")
        except Exception as e:
            app.logger.error(f"[course-create] trace_id={trace_id} json_parse_error: {str(e)}", exc_info=True)
            return make_response(jsonify({'error': f'Failed to parse LLM JSON: {str(e)}', 'trace_id': trace_id}), 502)

        # Post-process: enforce items_per_module length and 300-500 word, beginner-friendly content per item
        try:
            modules = course_obj.get('modules', [])
            if isinstance(modules, list):
                for m in modules:
                    clist = m.get('contentList')
                    if isinstance(clist, list):
                        if len(clist) > items_per_module:
                            m['contentList'] = clist[:items_per_module]
                            app.logger.info(f"[course-create] trace_id={trace_id} truncated contentList from {len(clist)} to {items_per_module}")
                        elif len(clist) < items_per_module and len(clist) > 0:
                            # Duplicate last item to reach desired count
                            last_item = clist[-1]
                            while len(m['contentList']) < items_per_module:
                                m['contentList'].append(last_item)
                            app.logger.info(f"[course-create] trace_id={trace_id} padded contentList to {items_per_module}")

                        # Enforce 300-500 words per content item
                        rewrites_used = 0
                        for item in m['contentList']:
                            try:
                                content_text = item.get('content', '') if isinstance(item, dict) else ''
                                word_count = _count_words(content_text)
                                if word_count == 0:
                                    continue
                                if word_count < 300:
                                    # Ask LLM to expand to 300-500 words, beginner-friendly
                                    time_spent = time.time() - request_start_time
                                    time_left = COURSE_REQUEST_TIME_BUDGET_S - time_spent
                                    if COURSE_REWRITE_MAX > 0 and rewrites_used < COURSE_REWRITE_MAX and time_left > 2.0:
                                        rewrite_prompt = {
                                            "instruction": "Rewrite this explanation to be beginner-friendly and 300-500 words. Use simple language, define terms, and include a short step-by-step and a relatable analogy. Return ONLY the rewritten text.",
                                            "topic": item.get('title') or m.get('title') or '',
                                            "original_text": content_text,
                                            "target_word_range": "300-500"
                                        }
                                        try:
                                            per_call_timeout = max(3.0, min(COURSE_REWRITE_TIMEOUT_S, time_left - 1.0))
                                            expansion = client.chat.completions.create(
                                                model=model_name,
                                                temperature=0.3,
                                                messages=[
                                                    {"role": "system", "content": "You improve course content for beginners."},
                                                    {"role": "user", "content": _json.dumps(rewrite_prompt)}
                                                ],
                                                timeout=per_call_timeout
                                            )
                                            new_text = (expansion.choices[0].message.content or '').strip()
                                            # Safety trim to 520 words max
                                            new_text = _trim_to_word_limit(new_text, 520)
                                            item['content'] = new_text
                                            rewrites_used += 1
                                        except Exception:
                                            # Fallback: leave as-is on failure
                                            pass
                                elif word_count > 500:
                                    # Trim softly to ~500 words (keep to 520 max to avoid abrupt cut)
                                    item['content'] = _trim_to_word_limit(content_text, 520)
                            except Exception:
                                continue
            # Attach pdfUrl if available
            if pdf_url:
                for m in course_obj.get('modules', []) or []:
                    if 'pdfUrl' not in m or not m['pdfUrl']:
                        m['pdfUrl'] = pdf_url
                    if isinstance(m.get('contentList'), list):
                        for item in m['contentList']:
                            if 'pdfUrl' not in item or not item['pdfUrl']:
                                item['pdfUrl'] = pdf_url
                            if 'mediaUrl' not in item or not item['mediaUrl']:
                                item['mediaUrl'] = pdf_url
                                item['mediaType'] = 'pdf'
        except Exception:
            app.logger.warning(f"[course-create] trace_id={trace_id} post_process_warning", exc_info=True)

        # Log final JSON (truncate for safety)
        try:
            _serialized = _json.dumps(course_obj, ensure_ascii=False)
            _max_log = 20000
            if len(_serialized) > _max_log:
                app.logger.info(f"[course-create] trace_id={trace_id} final_course_json_len={len(_serialized)} (truncated to {_max_log})")
                app.logger.info(f"[course-create] trace_id={trace_id} final_course_json_truncated={_serialized[:_max_log]}")
            else:
                app.logger.info(f"[course-create] trace_id={trace_id} final_course_json_len={len(_serialized)}")
                app.logger.info(f"[course-create] trace_id={trace_id} final_course_json={_serialized}")
        except Exception as _e:
            app.logger.warning(f"[course-create] trace_id={trace_id} final_json_log_error={str(_e)}")

        # Attach cohort_passwords if provided
        try:
            if cohort_passwords and isinstance(course_obj, dict):
                course_obj['cohort_passwords'] = cohort_passwords
        except Exception:
            app.logger.warning(f"[course-create] trace_id={trace_id} failed attaching cohort_passwords")

        app.logger.info(f"[course-create] trace_id={trace_id} success returning course")
        return jsonify({"course": course_obj, 'trace_id': trace_id}), 200

    except Exception as e:
        app.logger.error(f"[course-create] trace_id={trace_id if 'trace_id' in locals() else 'n/a'} unhandled_error: {str(e)}", exc_info=True)
        return make_response(jsonify({'error': str(e), 'trace_id': trace_id if 'trace_id' in locals() else None}), 500)


@app.route('/save-course', methods=['POST'])
def save_course():
    """
    Save a course JSON document to MongoDB 'Courses.coursedata'.
    Expected JSON body: { id_hash: string, course: CourseStruct, tags?: string[], title?: string }
    Returns: { course_id, message }
    """
    trace_id = str(uuid.uuid4())
    try:
        data = request.json or {}
        id_hash = data.get('id_hash')
        course = data.get('course')
        title_override = data.get('title')
        tags = data.get('tags', [])

        if not id_hash:
            return make_response(jsonify({'error': 'id_hash is required', 'trace_id': trace_id}), 400)
        if not course or not isinstance(course, dict):
            return make_response(jsonify({'error': 'course is required and must be an object', 'trace_id': trace_id}), 400)

        # Minimal validation of CourseStruct
        course_title = title_override or course.get('title') or 'Untitled Course'

        # Upsert based on (id_hash, title). Update existing JSON instead of inserting duplicates
        now = datetime.utcnow()
        filter_doc: Dict[str, Any] = {'id_hash': id_hash, 'title': course_title}
        update_doc: Dict[str, Any] = {
            '$set': {
                'course': course,
                'tags': tags if isinstance(tags, list) else [],
                'updated_at': now,
            },
            '$setOnInsert': {
                'id_hash': id_hash,
                'title': course_title,
                'created_at': now,
            }
        }

        result = course_data_collection.update_one(filter_doc, update_doc, upsert=True)

        # Determine course_id and message based on whether it was an insert or update
        if result.upserted_id:
            course_id = str(result.upserted_id)
            message = 'Course saved'
            app.logger.info(f"[save-course] trace_id={trace_id} inserted course_id={course_id} id_hash={id_hash} title={course_title}")
        else:
            existing = course_data_collection.find_one(filter_doc, {'_id': 1})
            course_id = str(existing.get('_id')) if existing else None
            message = 'Course updated'
            app.logger.info(f"[save-course] trace_id={trace_id} updated course_id={course_id} id_hash={id_hash} title={course_title}")

        return jsonify({'course_id': course_id, 'message': message, 'trace_id': trace_id}), 200
    except Exception as e:
        app.logger.error(f"[save-course] trace_id={trace_id} error: {str(e)}", exc_info=True)
        return make_response(jsonify({'error': str(e), 'trace_id': trace_id}), 500)


@app.route('/load-course', methods=['GET'])
def load_course():
    """
    Load a saved course from MongoDB 'Courses.coursedata'.
    Accepts query params: course_id or title, and optional id_hash for scoping.
    Returns: { course, course_id, title }
    """
    trace_id = str(uuid.uuid4())
    try:
        course_id = request.args.get('course_id')
        title = request.args.get('title')
        id_hash = request.args.get('id_hash')

        if not course_id and not title:
            return make_response(jsonify({'error': 'Provide course_id or title', 'trace_id': trace_id}), 400)

        query: Dict[str, Any] = {}
        if course_id:
            try:
                query['_id'] = ObjectId(course_id)
            except Exception:
                return make_response(jsonify({'error': 'Invalid course_id', 'trace_id': trace_id}), 400)
        if title:
            query['title'] = title
        if id_hash:
            query['id_hash'] = id_hash

        doc = course_data_collection.find_one(query)
        if not doc:
            return make_response(jsonify({'error': 'Course not found', 'trace_id': trace_id}), 404)

        # Prepare response without MongoDB internal _id
        response = {
            'course_id': str(doc.get('_id')),
            'title': doc.get('title'),
            'course': doc.get('course'),
            'tags': doc.get('tags', []),
            'created_at': doc.get('created_at'),
            'updated_at': doc.get('updated_at'),
            'trace_id': trace_id,
        }
        app.logger.info(f"[load-course] trace_id={trace_id} loaded course_id={response['course_id']} title={response['title']}")
        return jsonify(response), 200
    except Exception as e:
        app.logger.error(f"[load-course] trace_id={trace_id} error: {str(e)}", exc_info=True)
        return make_response(jsonify({'error': str(e), 'trace_id': trace_id}), 500)

@app.route('/list-courses', methods=['GET'])
def list_courses():
    id_hash = request.args.get('id_hash')
    q = {'id_hash': id_hash} if id_hash else {}
    cursor = course_data_collection.find(q, {'_id': 1, 'title': 1, 'created_at': 1, 'updated_at': 1, 'tags': 1})
    items = [{'course_id': str(doc['_id']), 'title': doc.get('title'), 'tags': doc.get('tags', []), 'created_at': doc.get('created_at'), 'updated_at': doc.get('updated_at')} for doc in cursor]
    return jsonify({'courses': items})

@app.route('/register-course', methods=['POST'])
def register_course():
    """
    Register a user to a course by validating a cohort password.
    Expected JSON body: { id_hash: string, course_id: string, cohort_password: string }
    Success: adds the course ObjectId to user document's registered_courses array.
    """
    trace_id = str(uuid.uuid4())
    try:
        data = request.json or {}
        id_hash = data.get('id_hash')
        course_id = data.get('course_id')
        supplied_password = data.get('cohort_password') or data.get('password')

        if not id_hash or not course_id or supplied_password is None:
            return make_response(jsonify({'error': 'id_hash, course_id, and cohort_password are required', 'trace_id': trace_id}), 400)

        # Ensure user exists
        user = userlist.find_one({'id_hash': id_hash})
        if not user:
            return make_response(jsonify({'error': 'User not found', 'trace_id': trace_id}), 404)

        # Validate and fetch course
        try:
            course_oid = ObjectId(course_id)
        except Exception:
            return make_response(jsonify({'error': 'Invalid course_id', 'trace_id': trace_id}), 400)

        doc = course_data_collection.find_one({'_id': course_oid})
        if not doc:
            return make_response(jsonify({'error': 'Course not found', 'trace_id': trace_id}), 404)

        course_obj = doc.get('course') or {}
        cohort_passwords = course_obj.get('cohort_passwords') if isinstance(course_obj, dict) else None
        if not isinstance(cohort_passwords, dict) or not cohort_passwords:
            return make_response(jsonify({'error': 'Course does not support cohort registration', 'trace_id': trace_id}), 400)

        # Check password against any cohort value
        matched_cohort = None
        try:
            for name, pwd in cohort_passwords.items():
                name_str = str(name).strip()
                pwd_str = '' if pwd is None else str(pwd)
                if supplied_password == pwd_str:
                    matched_cohort = name_str or None
                    break
        except Exception:
            pass

        if not matched_cohort:
            return make_response(jsonify({'error': 'Invalid cohort password', 'trace_id': trace_id}), 403)

        # Persist registration with cohort mapping inside registered_courses tag
        now = datetime.utcnow()

        # 1) If an object entry already exists, update its cohort
        updated = userlist.update_one(
            {
                'id_hash': id_hash,
                'registered_courses': {
                    '$elemMatch': {
                        'course_id': course_oid
                    }
                }
            },
            {
                '$set': {
                    'registered_courses.$.cohort': matched_cohort,
                    'registered_courses.$.registered_at': now
                }
            }
        )

        action = 'updated' if updated.modified_count > 0 else 'inserted'

        if updated.modified_count == 0:
            # 2) If there's a raw ObjectId inside the array, convert it to an object with cohort
            pulled = userlist.update_one(
                {
                    'id_hash': id_hash,
                    'registered_courses': course_oid
                },
                {
                    '$pull': {
                        'registered_courses': course_oid
                    }
                }
            )

            # 3) Push new structured entry (idempotent after pull or if none existed)
            userlist.update_one(
                {'id_hash': id_hash},
                {
                    '$addToSet': {
                        'registered_courses': {
                            'course_id': course_oid,
                            'cohort': matched_cohort,
                            'registered_at': now
                        }
                    }
                }
            )

        app.logger.info(f"[register-course] trace_id={trace_id} id_hash={id_hash} course_id={course_id} cohort={matched_cohort} action={action}")
        return jsonify({'success': True, 'registered': True, 'course_id': course_id, 'cohort': matched_cohort, 'trace_id': trace_id}), 200
    except Exception as e:
        app.logger.error(f"[register-course] trace_id={trace_id} error: {str(e)}", exc_info=True)
        return make_response(jsonify({'error': str(e), 'trace_id': trace_id}), 500)

@app.route('/set-user-params', methods=['POST'])
def set_user_params():
    data = request.json
    id_hash = data.get('id_hash')
    experiencelevel = data.get('experiencelevel')
    age = data.get('age')
    questioningstyle = data.get('questioningstyle')
    interactionspeed = data.get('interactionspeed')
    feedbackstyle = data.get('feedbackstyle')
    socraticdepth = data.get('socraticdepth')

    if not id_hash:
        return jsonify({"error": "ID hash is required"}), 400

    if id_hash:
        user = user_params_collection.find_one({'id_hash': id_hash})
        if user:
            if experiencelevel:
                user_params_collection.update_one({'id_hash': id_hash}, {'$set': {'experiencelevel': experiencelevel}})
            if age:
                user_params_collection.update_one({'id_hash': id_hash}, {'$set': {'age': age}})
            if questioningstyle:
                user_params_collection.update_one({'id_hash': id_hash}, {'$set': {'questioningstyle': questioningstyle}})
            if interactionspeed:
                user_params_collection.update_one({'id_hash': id_hash}, {'$set': {'interactionspeed': interactionspeed}})
            if feedbackstyle:
                user_params_collection.update_one({'id_hash': id_hash}, {'$set': {'feedbackstyle': feedbackstyle}})
            if socraticdepth:
                user_params_collection.update_one({'id_hash': id_hash}, {'$set': {'socraticdepth': socraticdepth}})
        else:
            user_params_collection.insert_one({'id_hash': id_hash, 'experiencelevel': "novice", 'age': "20", 'questioningstyle': "open-ended", 'interactionspeed': "slow", 'feedbackstyle': "constructive", 'socraticdepth': "high"})

    return jsonify({"message": "User params set successfully"}), 200

#adds an element to a list of portfolio ids on the user document
@app.route('/add-portfolio-id', methods=['POST'])
def add_portfolio_id():
    data = request.json
    id_hash = data.get('id_hash')
    portfolio_id = data.get('portfolio_id')

    if not id_hash:
        return jsonify({"error": "ID hash is required"}), 400

    if not portfolio_id:
        return jsonify({"error": "Portfolio ID is required"}), 400

    userlist.update_one({'id_hash': id_hash}, {'$push': {'portfolio_ids': portfolio_id}})

    return jsonify({"message": "Portfolio ID added successfully"}), 200

#removes an element from a list of portfolio ids on the user document
@app.route('/remove-portfolio-id', methods=['POST'])
def remove_portfolio_id():
    data = request.json
    id_hash = data.get('id_hash')
    portfolio_id = data.get('portfolio_id')
    portfolio_user_id = data.get('portfolio_user_id')

    if not id_hash:
        return jsonify({"error": "ID hash is required"}), 400

    if not portfolio_id:
        return jsonify({"error": "Portfolio ID is required"}), 400

    userlist.update_one({'id_hash': id_hash}, {'$pull': {'portfolio_ids': portfolio_id}})

    #delete the document with the object id that is the same as the portfolio_id
    portfolio_list.delete_one({'_id': ObjectId(portfolio_id)})

    #delete the document with the user_id the same as the id_hash
    schwab_tokens_list.delete_one({'user_id': portfolio_user_id})

    return jsonify({"message": "Portfolio ID removed successfully"}), 200


@app.route('/get-portfolio-ids', methods=['GET'])
def get_portfolio_ids():
    id_hash = request.args.get('id_hash')

    if not id_hash:
        return jsonify({"error": "ID hash is required"}), 400

    user_doc = userlist.find_one({'id_hash': id_hash}, {'portfolio_ids': 1, '_id': 0})
    
    # If user not found or no portfolio_ids field, return empty array
    if not user_doc or 'portfolio_ids' not in user_doc:
        return jsonify({"portfolio_ids": []}), 200
    
    # Return the portfolio_ids in the expected format
    return jsonify({"portfolio_ids": user_doc['portfolio_ids']}), 200

@app.route('/track-credit-usage', methods=['POST'])
def track_credit_usage():
    data = request.json
    id_hash = data.get('id_hash')
    feature_type = data.get('feature_type')  # 'analysis', 'chat', 'courses', or 'portfolio'

    if not id_hash:
        return make_response(jsonify({'error': 'ID hash is required'}), 400)
    
    if not feature_type or feature_type not in ['analysis', 'chat', 'courses', 'portfolio']:
        return make_response(jsonify({'error': 'Valid feature_type is required (analysis, chat, courses, portfolio)'}), 400)

    # Get current UTC timestamp
    current_time = datetime.utcnow()
    
    # Create log document
    log_entry = {
        'timestamp': current_time,
        'feature_type': feature_type,
        'day': current_time.day,
        'month': current_time.month,
        'year': current_time.year,
        'hour': current_time.hour,
        'minute': current_time.minute,
        'second': current_time.second
    }

    # Initialize credit_usage and credit_logs if they don't exist
    user = userlist.find_one({'id_hash': id_hash})
    if not user:
        return make_response(jsonify({'error': 'User not found'}), 404)

    # Initialize credit_usage and credit_logs fields if they don't exist
    if 'credit_usage' not in user or 'credit_logs' not in user:
        userlist.update_one(
            {'id_hash': id_hash},
            {'$set': {
                'credit_usage': {
                    'analysis': 0,
                    'chat': 0,
                    'courses': 0,
                    'portfolio': 0
                },
                'credit_logs': []
            }}
        )

    # Update both credit_usage and add new log entry
    result = userlist.update_one(
        {'id_hash': id_hash},
        {
            '$inc': {f'credit_usage.{feature_type}': 1},
            '$push': {'credit_logs': log_entry}
        }
    )

    if result.modified_count == 0:
        return make_response(jsonify({'error': 'Failed to update credit usage'}), 500)

    # Get updated user document
    updated_user = userlist.find_one({'id_hash': id_hash}, {'_id': 0})
    return jsonify({
        'message': 'Credit usage updated successfully',
        'credit_usage': updated_user.get('credit_usage', {}),
        'latest_log': log_entry
    })

@app.route('/log-score', methods=['POST'])
@app.route('/log-scores', methods=['POST'])
def log_score():
    """
    Log or update a user's score for a specific course/module/content.
    Expected JSON body:
      {
        id_hash: string,
        course_id: string (Mongo ObjectId),
        module: string|number,
        content: string|number,
        score: number
      }
    Behavior:
      - Overwrites existing score log with the same (course_id, module, content)
      - Creates score_logs array if missing
    """
    trace_id = str(uuid.uuid4())
    try:
        data = request.json or {}
        id_hash = data.get('id_hash')
        course_id = data.get('course_id')
        course_title = data.get('course_title') or data.get('title')
        module_val = data.get('module')
        content_val = data.get('content')
        score_val = data.get('score')

        if (
            id_hash is None
            or module_val is None
            or content_val is None
            or score_val is None
            or (course_id is None and not course_title)
        ):
            return make_response(jsonify({
                'error': 'id_hash, module, content, score, and (course_id or course_title) are required',
                'trace_id': trace_id
            }), 400)

        # Validate user
        user = userlist.find_one({'id_hash': id_hash})
        if not user:
            return make_response(jsonify({'error': 'User not found', 'trace_id': trace_id}), 404)

        # Resolve course ObjectId either from course_id or by (course_title, id_hash)
        # Resolve course ObjectId either from course_id or by (course_title, id_hash)
        course_oid = None
        if course_id is not None:
            try:
                course_oid = ObjectId(course_id)
            except Exception:
                app.logger.info(f"[log-score] trace_id={trace_id} invalid course_id; attempting title fallback")

        if course_oid is None:
            if not course_title:
                return make_response(jsonify({'error': 'Provide a valid course_id or course_title', 'trace_id': trace_id}), 400)

            # 1) Try title scoped by id_hash (preferred)
            query_scoped: Dict[str, Any] = {'title': course_title, 'id_hash': id_hash}
            doc = course_data_collection.find_one(query_scoped, {'_id': 1}) or None

            # 2) Fallback: try title only (any owner)
            if not doc:
                doc = course_data_collection.find_one({'title': course_title}, {'_id': 1}) or None

            # 3) If still not found, create a minimal stub so default courses can log scores
            if not doc:
                now = datetime.utcnow()
                stub = {
                    'title': course_title,
                    'description': f'Default course: {course_title}',
                    'modules': [],  # optional; you can omit/keep minimal
                    'id_hash': id_hash,  # tie to current user for provenance
                    'created_at': now,
                    'updated_at': now,
                }
                insert_res = course_data_collection.insert_one(stub)
                course_oid = insert_res.inserted_id
                app.logger.info(f"[log-score] trace_id={trace_id} created stub course for title='{course_title}' oid={course_oid}")
            else:
                course_oid = doc['_id']

        # Normalize values
        try:
            numeric_score = float(score_val)
        except Exception:
            return make_response(jsonify({'error': 'score must be a number', 'trace_id': trace_id}), 400)

        now = datetime.utcnow()

        # Try to fetch existing log entry for running average update
        existing = userlist.find_one(
            {
                'id_hash': id_hash,
                'score_logs': {
                    '$elemMatch': {
                        'course_id': course_oid,
                        'module': module_val,
                        'content': content_val
                    }
                }
            },
            {'score_logs.$': 1}
        )

        if existing and isinstance(existing.get('score_logs'), list) and existing['score_logs']:
            current = existing['score_logs'][0]
            prev_count = 0
            prev_avg = None
            try:
                prev_count = int(current.get('questions_answered') or 0)
            except Exception:
                prev_count = 0
            try:
                # Prefer stored average_score; fallback to score field if present
                if 'average_score' in current:
                    prev_avg = float(current.get('average_score') or 0)
                elif 'score' in current:
                    prev_avg = float(current.get('score') or 0)
                else:
                    prev_avg = None
            except Exception:
                prev_avg = None

            # Initialize if missing
            if prev_avg is None:
                prev_avg = numeric_score
                prev_count = 0

            new_count = prev_count + 1
            new_avg = ((prev_avg * prev_count) + numeric_score) / new_count if new_count > 0 else numeric_score

            update_result = userlist.update_one(
                {
                    'id_hash': id_hash,
                    'score_logs': {
                        '$elemMatch': {
                            'course_id': course_oid,
                            'module': module_val,
                            'content': content_val
                        }
                    }
                },
                {
                    '$set': {
                        'score_logs.$.latest_score': numeric_score,
                        'score_logs.$.average_score': new_avg,
                        'score_logs.$.questions_answered': new_count,
                        'score_logs.$.score': new_avg,
                        'score_logs.$.updated_at': now
                    }
                }
            )
            action = 'updated'
        else:
            # Insert new log entry if none existed
            log_entry = {
                'course_id': course_oid,
                'module': module_val,
                'content': content_val,
                'latest_score': numeric_score,
                'average_score': numeric_score,
                'questions_answered': 1,
                'score': numeric_score,
                'updated_at': now
            }
            userlist.update_one(
                {'id_hash': id_hash},
                {'$push': {'score_logs': log_entry}}
            )
            action = 'inserted'

        app.logger.info(f"[log-score] trace_id={trace_id} id_hash={id_hash} course_id={course_id} module={module_val} content={content_val} action={action}")

        return jsonify({'success': True, 'action': action, 'trace_id': trace_id}), 200
    except Exception as e:
        app.logger.error(f"[log-score] trace_id={trace_id} error: {str(e)}", exc_info=True)
        return make_response(jsonify({'error': str(e), 'trace_id': trace_id}), 500)

@app.route('/load-filtered-student-data', methods=['POST'])
def load_filtered_student_data():
    """
    Load per-student scores for a given course and specific module/content.
    Expected JSON body:
      {
        course_id?: string (Mongo ObjectId),
        course_title?: string,
        module: string|number,
        content: string|number
      }
    Returns: { students: StudentScore[], course_id, course_title }
    StudentScore shape:
      {
        student: string,
        cohort: string,
        courseTitle: string,
        moduleTitle: string,
        contentTitle: string,
        averageScore: number, // 0-10
        questionsAnswered: number
      }
    """
    trace_id = str(uuid.uuid4())
    try:
        data = request.json or {}
        course_id = data.get('course_id')
        course_title = data.get('course_title') or data.get('title')
        module_val = data.get('module')
        content_val = data.get('content')

        if (course_id is None and not course_title) or module_val is None or content_val is None:
            return make_response(jsonify({'error': 'Provide module, content, and either course_id or course_title', 'trace_id': trace_id}), 400)

        # Resolve course ObjectId and canonical title
        resolved_oid = None
        resolved_title = None
        if course_id is not None:
            try:
                resolved_oid = ObjectId(course_id)
            except Exception:
                app.logger.info(f"[load-filtered-student-data] trace_id={trace_id} invalid course_id; will try title fallback")
        if resolved_oid is None:
            if not course_title:
                return make_response(jsonify({'error': 'Invalid course_id and missing course_title', 'trace_id': trace_id}), 400)
            query: Dict[str, Any] = {'title': course_title}
            try:
                # Prefer most recently updated
                cursor = course_data_collection.find(query).sort([('updated_at', -1), ('created_at', -1)]).limit(1)
                doc = next(cursor, None)
            except Exception:
                doc = course_data_collection.find_one(query)
            if not doc or not doc.get('_id'):
                return make_response(jsonify({'error': 'Course not found by title', 'trace_id': trace_id}), 404)
            resolved_oid = doc.get('_id')
            resolved_title = doc.get('title') or course_title
        else:
            # Lookup title for completeness
            doc = course_data_collection.find_one({'_id': resolved_oid}, {'title': 1})
            resolved_title = (doc or {}).get('title') or course_title or ''

        # Find users who have score logs for this specific (course, module, content)
        # Pull minimal fields to compute results
        candidates = userlist.find(
            {
                'score_logs': {
                    '$elemMatch': {
                        'course_id': resolved_oid,
                        'module': module_val,
                        'content': content_val
                    }
                }
            },
            {
                '_id': 1,
                'id_hash': 1,
                'email': 1,
                'score_logs': 1,
                'registered_courses': 1
            }
        )

        students: List[Dict[str, Any]] = []
        for user_doc in candidates:
            user_email = user_doc.get('email')
            user_identifier = user_email or user_doc.get('id_hash') or 'Unknown'
            # Determine cohort from registered_courses (supports both legacy ObjectId and structured object entries)
            cohort_value = 'Unknown'
            try:
                rc_list = user_doc.get('registered_courses')
                if isinstance(rc_list, list):
                    for entry in rc_list:
                        if isinstance(entry, dict) and entry.get('course_id') == resolved_oid:
                            maybe_cohort = entry.get('cohort')
                            if isinstance(maybe_cohort, str) and maybe_cohort.strip():
                                cohort_value = maybe_cohort.strip()
                                break
                        # Legacy case: bare ObjectId
                        if entry == resolved_oid:
                            cohort_value = 'Unknown'
            except Exception:
                pass

            # Collect all matching logs for safety (though we overwrite, duplicates may exist historically)
            logs = user_doc.get('score_logs') or []
            matched_scores: List[float] = []
            for log in logs:
                try:
                    if (
                        isinstance(log, dict) and
                        log.get('course_id') == resolved_oid and
                        log.get('module') == module_val and
                        log.get('content') == content_val
                    ):
                        # Prefer aggregated fields if present
                        if 'questions_answered' in log and 'average_score' in log:
                            qa = int(log.get('questions_answered') or 0)
                            avg = float(log.get('average_score') or 0)
                            if qa > 0:
                                matched_scores.extend([avg] * qa)
                        else:
                            val = float(log.get('score'))
                            matched_scores.append(val)
                except Exception:
                    continue

            if not matched_scores:
                continue

            questions_answered = len(matched_scores)
            avg_score = sum(matched_scores) / questions_answered if questions_answered else 0.0
            # Round to 1 decimal to mirror frontend rounding
            avg_score = round(avg_score * 10) / 10.0

            students.append({
                'student': user_identifier,
                'cohort': cohort_value,
                'courseTitle': resolved_title,
                'moduleTitle': module_val,
                'contentTitle': content_val,
                'averageScore': avg_score,
                'questionsAnswered': questions_answered
            })

        return jsonify({
            'students': students,
            'course_id': str(resolved_oid),
            'course_title': resolved_title,
            'trace_id': trace_id
        }), 200
    except Exception as e:
        app.logger.error(f"[load-filtered-student-data] trace_id={trace_id} error: {str(e)}", exc_info=True)
        return make_response(jsonify({'error': str(e), 'trace_id': trace_id}), 500)

if __name__ == '__main__':
    # This will be run by gunicorn in production
    app.run(host='0.0.0.0', port=5000)
