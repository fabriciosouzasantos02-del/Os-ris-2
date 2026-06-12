import { initializeApp, getApps, getApp } from "firebase/app";
import { 
  getFirestore, 
  collection, 
  doc, 
  getDoc, 
  setDoc, 
  updateDoc, 
  deleteDoc, 
  getDocs, 
  query, 
  where, 
  addDoc,
  onSnapshot
} from "firebase/firestore";
import { 
  getAuth,
  signInWithPopup,
  GoogleAuthProvider,
  FacebookAuthProvider,
  signInWithEmailAndPassword,
  createUserWithEmailAndPassword,
  sendPasswordResetEmail,
  signOut,
  updateProfile,
  onAuthStateChanged
} from "firebase/auth";

// -------------------------------------------------------------------------
// COMPLIANT FIRESTORE ERROR HANDLING INTERFACES
// -------------------------------------------------------------------------
export enum OperationType {
  CREATE = 'create',
  UPDATE = 'update',
  DELETE = 'delete',
  LIST = 'list',
  GET = 'get',
  WRITE = 'write',
}

export interface FirestoreErrorInfo {
  error: string;
  operationType: OperationType;
  path: string | null;
  authInfo: {
    userId?: string | null;
    email?: string | null;
    emailVerified?: boolean | null;
    isAnonymous?: boolean | null;
    tenantId?: string | null;
    providerInfo?: {
      providerId?: string | null;
      email?: string | null;
    }[];
  }
}

export function handleFirestoreError(error: unknown, operationType: OperationType, path: string | null): never {
  const auth = getFirebaseAuth();
  const errInfo: FirestoreErrorInfo = {
    error: error instanceof Error ? error.message : String(error),
    authInfo: {
      userId: auth?.currentUser?.uid || null,
      email: auth?.currentUser?.email || null,
      emailVerified: auth?.currentUser?.emailVerified || null,
      isAnonymous: auth?.currentUser?.isAnonymous || null,
      tenantId: auth?.currentUser?.tenantId || null,
      providerInfo: auth?.currentUser?.providerData?.map(provider => ({
        providerId: provider.providerId,
        email: provider.email,
      })) || []
    },
    operationType,
    path
  };
  console.error('[Securitizao] Erro do Firestore Detectado: ', JSON.stringify(errInfo));
  throw new Error(JSON.stringify(errInfo));
}

// Lazy-loaded or optionally fallback firebase configuration
let appInstance: any = null;
let dbInstance: any = null;
let authInstance: any = null;

// Standard firebase configuration or dynamic env fallback
const firebaseConfig = {
  apiKey: import.meta.env.VITE_FIREBASE_API_KEY || "",
  authDomain: import.meta.env.VITE_FIREBASE_AUTH_DOMAIN || "",
  projectId: import.meta.env.VITE_FIREBASE_PROJECT_ID || "",
  storageBucket: import.meta.env.VITE_FIREBASE_STORAGE_BUCKET || "",
  messagingSenderId: import.meta.env.VITE_FIREBASE_MESSAGING_SENDER_ID || "",
  appId: import.meta.env.VITE_FIREBASE_APP_ID || ""
};

// Check if we can safely initialize firebase Client
const hasConfig = !!(firebaseConfig.apiKey && firebaseConfig.projectId);

export function getFirebaseApp() {
  if (!hasConfig) return null;
  if (!appInstance) {
    if (getApps().length === 0) {
      appInstance = initializeApp(firebaseConfig);
    } else {
      appInstance = getApp();
    }
  }
  return appInstance;
}

export function getFirestoreDB() {
  if (!hasConfig) return null;
  if (!dbInstance) {
    const app = getFirebaseApp();
    if (app) {
      dbInstance = getFirestore(app);
    }
  }
  return dbInstance;
}

export function getFirebaseAuth() {
  if (!hasConfig) return null;
  if (!authInstance) {
    const app = getFirebaseApp();
    if (app) {
      authInstance = getAuth(app);
    }
  }
  return authInstance;
}

// Ensure first connection is validated on startup
async function validateFirestoreConnection() {
  const db = getFirestoreDB();
  if (db) {
    try {
      const { getDocFromServer } = await import('firebase/firestore');
      await getDocFromServer(doc(db, 'test', 'connection'));
      console.log("[Firestore] Teste de conexão estabelecido com sucesso.");
    } catch (error) {
      if (error instanceof Error && error.message.includes('the client is offline')) {
        console.error("Please check your Firebase configuration. The client is offline.");
      }
    }
  }
}
validateFirestoreConnection();

// -------------------------------------------------------------------------
// HYBRID SYNCRONIZER LAYER FOR DATA PERSISTENCE & REAL-TIME SYNC
// -------------------------------------------------------------------------
export interface UserProfileData {
  email: string;
  name: string;
  birthDate: string;
  birthTime?: string;
  birthCity: string;
  profilePhoto?: string;
  isPremium?: boolean;
  hasCreatedMap?: boolean;
  scorePoints?: number;
}

export interface ExtraMapItem {
  id: string;
  userId: string;
  label: string;
  birthDate: string;
  birthTime?: string;
  birthCity: string;
  createdAt: string;
}

export interface DreamLogItem {
  id: string;
  userId: string;
  title: string;
  text: string;
  interpretation: string;
  sentiment: string;
  date: string;
}

// 1. Core Profile Real-Time Synchronizers
export async function saveProfileToDatabase(email: string, profile: UserProfileData) {
  const mailKey = email.toLowerCase().trim();
  if (!mailKey) return;
  
  localStorage.setItem("orbi_user_profile", JSON.stringify(profile));
  
  const db = getFirestoreDB();
  if (db) {
    const path = `users/${mailKey}`;
    try {
      const userRef = doc(db, "users", mailKey);
      await setDoc(userRef, {
        ...profile,
        email: mailKey,
        updatedAt: new Date().toISOString()
      }, { merge: true });
      console.log("[Sync] Perfil de usuário integrado na nuvem.");
    } catch (e) {
      console.warn("[Sync] Gravação em nuvem adiada. Backup local ativo.");
      handleFirestoreError(e, OperationType.WRITE, path);
    }
  }
}

export async function loadProfileFromDatabase(email: string): Promise<UserProfileData | null> {
  const mailKey = email.toLowerCase().trim();
  if (!mailKey) return null;
  
  const db = getFirestoreDB();
  if (db) {
    const path = `users/${mailKey}`;
    try {
      const userRef = doc(db, "users", mailKey);
      const snap = await getDoc(userRef);
      if (snap.exists()) {
        const raw = snap.data() as UserProfileData;
        localStorage.setItem("orbi_user_profile", JSON.stringify(raw));
        return raw;
      }
    } catch (e) {
      console.warn("[Sync] Leitura em nuvem falhou. Carregando cache local.");
      handleFirestoreError(e, OperationType.GET, path);
    }
  }
  
  const saved = localStorage.getItem("orbi_user_profile");
  if (saved) {
    try {
      const parsed = JSON.parse(saved);
      if (parsed && parsed.email?.toLowerCase().trim() === mailKey) {
        return parsed;
      }
    } catch {}
  }
  return null;
}

// Real-Time Listener for User Profile
export function subscribeToUserProfile(email: string, onUpdate: (profile: UserProfileData | null) => void, onError?: (err: Error) => void) {
  const mailKey = email.toLowerCase().trim();
  const db = getFirestoreDB();
  if (!db || !mailKey) {
    // If offline / no configuration, return dummy unsubscribe
    return () => {};
  }

  const path = `users/${mailKey}`;
  const docRef = doc(db, "users", mailKey);

  return onSnapshot(docRef, (docSnap) => {
    if (docSnap.exists()) {
      const data = docSnap.data() as UserProfileData;
      localStorage.setItem("orbi_user_profile", JSON.stringify(data));
      onUpdate(data);
    } else {
      onUpdate(null);
    }
  }, (error) => {
    console.error("[SnapSync] Erro no snapshot do perfil:", error);
    try {
      handleFirestoreError(error, OperationType.GET, path);
    } catch (transformed) {
      if (onError && transformed instanceof Error) onError(transformed);
    }
  });
}

// 2. Extra Maps Sync & Real-Time Subscriber
export async function saveExtraMapToDatabase(email: string, extraMap: ExtraMapItem) {
  const mailKey = email.toLowerCase().trim();
  if (!mailKey) return;

  const savedList = localStorage.getItem("orbi_extra_maps");
  let currentList: ExtraMapItem[] = [];
  try {
    currentList = savedList ? JSON.parse(savedList) : [];
  } catch {}
  currentList = currentList.filter(m => m.id !== extraMap.id);
  currentList.push(extraMap);
  localStorage.setItem("orbi_extra_maps", JSON.stringify(currentList));

  const db = getFirestoreDB();
  if (db) {
    const path = `users/${mailKey}/extraMaps/${extraMap.id}`;
    try {
      const mapRef = doc(db, "users", mailKey, "extraMaps", extraMap.id);
      await setDoc(mapRef, {
        ...extraMap,
        userId: mailKey
      });
    } catch (e) {
      console.warn("[Sync] Gravação de mapa extra diferida.");
      handleFirestoreError(e, OperationType.WRITE, path);
    }
  }
}

export async function deleteExtraMapFromDatabase(email: string, mapId: string) {
  const mailKey = email.toLowerCase().trim();
  if (!mailKey) return;

  const savedList = localStorage.getItem("orbi_extra_maps");
  let currentList: ExtraMapItem[] = [];
  try {
    currentList = savedList ? JSON.parse(savedList) : [];
  } catch {}
  currentList = currentList.filter(m => m.id !== mapId);
  localStorage.setItem("orbi_extra_maps", JSON.stringify(currentList));

  const db = getFirestoreDB();
  if (db) {
    const path = `users/${mailKey}/extraMaps/${mapId}`;
    try {
      const mapRef = doc(db, "users", mailKey, "extraMaps", mapId);
      await deleteDoc(mapRef);
    } catch (e) {
      handleFirestoreError(e, OperationType.DELETE, path);
    }
  }
}

export async function loadExtraMapsFromDatabase(email: string): Promise<ExtraMapItem[]> {
  const mailKey = email.toLowerCase().trim();
  if (!mailKey) return [];

  const db = getFirestoreDB();
  if (db) {
    const path = `users/${mailKey}/extraMaps`;
    try {
      const colRef = collection(db, "users", mailKey, "extraMaps");
      const snap = await getDocs(colRef);
      const results: ExtraMapItem[] = [];
      snap.forEach((docSnap) => {
        results.push(docSnap.data() as ExtraMapItem);
      });
      if (results.length > 0) {
        localStorage.setItem("orbi_extra_maps", JSON.stringify(results));
        return results;
      }
    } catch (e) {
      handleFirestoreError(e, OperationType.GET, path);
    }
  }

  const savedList = localStorage.getItem("orbi_extra_maps");
  if (savedList) {
    try {
      return JSON.parse(savedList);
    } catch {}
  }
  return [];
}

// Real-Time Listener for User Extra Maps Collection
export function subscribeToExtraMaps(email: string, onUpdate: (maps: ExtraMapItem[]) => void, onError?: (err: Error) => void) {
  const mailKey = email.toLowerCase().trim();
  const db = getFirestoreDB();
  if (!db || !mailKey) {
    return () => {};
  }

  const path = `users/${mailKey}/extraMaps`;
  const collectionRef = collection(db, "users", mailKey, "extraMaps");

  return onSnapshot(collectionRef, (snapshot) => {
    const results: ExtraMapItem[] = [];
    snapshot.forEach((snap) => {
      results.push(snap.data() as ExtraMapItem);
    });
    localStorage.setItem("orbi_extra_maps", JSON.stringify(results));
    onUpdate(results);
  }, (error) => {
    console.error("[SnapSync] Erro no snapshot de extraMaps:", error);
    try {
      handleFirestoreError(error, OperationType.GET, path);
    } catch (transformed) {
      if (onError && transformed instanceof Error) onError(transformed);
    }
  });
}

// 3. Dreams Sync & Real-Time Subscriber
export async function saveDreamToDatabase(email: string, dream: DreamLogItem) {
  const mailKey = email.toLowerCase().trim();
  if (!mailKey) return;

  const savedList = localStorage.getItem("star_map_dreams_v2");
  let currentList: DreamLogItem[] = [];
  try {
    currentList = savedList ? JSON.parse(savedList) : [];
  } catch {}
  currentList = currentList.filter(d => d.id !== dream.id);
  currentList.push(dream);
  localStorage.setItem("star_map_dreams_v2", JSON.stringify(currentList));

  const db = getFirestoreDB();
  if (db) {
    const path = `users/${mailKey}/dreams/${dream.id}`;
    try {
      const dreamRef = doc(db, "users", mailKey, "dreams", dream.id);
      await setDoc(dreamRef, {
        ...dream,
        userId: mailKey
      });
    } catch (e) {
      handleFirestoreError(e, OperationType.WRITE, path);
    }
  }
}

export async function deleteDreamFromDatabase(email: string, dreamId: string) {
  const mailKey = email.toLowerCase().trim();
  if (!mailKey) return;

  const savedList = localStorage.getItem("star_map_dreams_v2");
  let currentList: DreamLogItem[] = [];
  try {
    currentList = savedList ? JSON.parse(savedList) : [];
  } catch {}
  currentList = currentList.filter(d => d.id !== dreamId);
  localStorage.setItem("star_map_dreams_v2", JSON.stringify(currentList));

  const db = getFirestoreDB();
  if (db) {
    const path = `users/${mailKey}/dreams/${dreamId}`;
    try {
      const dreamRef = doc(db, "users", mailKey, "dreams", dreamId);
      await deleteDoc(dreamRef);
    } catch (e) {
      handleFirestoreError(e, OperationType.DELETE, path);
    }
  }
}

export async function loadDreamsFromDatabase(email: string): Promise<DreamLogItem[]> {
  const mailKey = email.toLowerCase().trim();
  if (!mailKey) return [];

  const db = getFirestoreDB();
  if (db) {
    const path = `users/${mailKey}/dreams`;
    try {
      const colRef = collection(db, "users", mailKey, "dreams");
      const snap = await getDocs(colRef);
      const results: DreamLogItem[] = [];
      snap.forEach((docSnap) => {
        results.push(docSnap.data() as DreamLogItem);
      });
      if (results.length > 0) {
        localStorage.setItem("star_map_dreams_v2", JSON.stringify(results));
        return results;
      }
    } catch (e) {
      handleFirestoreError(e, OperationType.GET, path);
    }
  }

  const savedList = localStorage.getItem("star_map_dreams_v2");
  if (savedList) {
    try {
      return JSON.parse(savedList);
    } catch {}
  }
  return [];
}

// Real-Time Listener for Dreams Subcollection
export function subscribeToDreams(email: string, onUpdate: (dreams: DreamLogItem[]) => void, onError?: (err: Error) => void) {
  const mailKey = email.toLowerCase().trim();
  const db = getFirestoreDB();
  if (!db || !mailKey) {
    return () => {};
  }

  const path = `users/${mailKey}/dreams`;
  const collectionRef = collection(db, "users", mailKey, "dreams");

  return onSnapshot(collectionRef, (snapshot) => {
    const results: DreamLogItem[] = [];
    snapshot.forEach((snap) => {
      results.push(snap.data() as DreamLogItem);
    });
    localStorage.setItem("star_map_dreams_v2", JSON.stringify(results));
    onUpdate(results);
  }, (error) => {
    console.error("[SnapSync] Erro no snapshot de dreams:", error);
    try {
      handleFirestoreError(error, OperationType.GET, path);
    } catch (transformed) {
      if (onError && transformed instanceof Error) onError(transformed);
    }
  });
}

// -------------------------------------------------------------------------
// REAL FIREBASE AUTHENTICATION FLOWS (WITH LOCAL SECURE RECONCILIATION)
// -------------------------------------------------------------------------
export async function registerWithEmailFirebase(email: string, pass: string): Promise<any> {
  const auth = getFirebaseAuth();
  if (!auth) {
    throw new Error("O Firebase Auth não está configurado. Cadastrando no modo offline nativo.");
  }
  try {
    const cred = await createUserWithEmailAndPassword(auth, email.trim(), pass);
    return cred.user;
  } catch (error) {
    console.error("[Auth] Falha no cadastro Firebase:", error);
    throw error;
  }
}

export async function loginWithEmailFirebase(email: string, pass: string): Promise<any> {
  const auth = getFirebaseAuth();
  if (!auth) {
    throw new Error("O Firebase Auth não está configurado. Tente o acesso offline.");
  }
  try {
    const cred = await signInWithEmailAndPassword(auth, email.trim(), pass);
    return cred.user;
  } catch (error) {
    console.error("[Auth] Falha no login Firebase:", error);
    throw error;
  }
}

export async function loginWithGoogleFirebase(): Promise<any> {
  const auth = getFirebaseAuth();
  if (!auth) {
    throw new Error("Google Sign-In indesejado porque o Firebase não está configurado.");
  }
  try {
    const provider = new GoogleAuthProvider();
    const cred = await signInWithPopup(auth, provider);
    return cred.user;
  } catch (error) {
    console.error("[Auth] Falha no Google Access:", error);
    throw error;
  }
}

export async function logoutWithFirebase(): Promise<void> {
  const auth = getFirebaseAuth();
  if (auth) {
    await signOut(auth);
  }
}

export function subscribeToAuthChanges(callback: (user: any) => void) {
  const auth = getFirebaseAuth();
  if (!auth) return () => {};
  return onAuthStateChanged(auth, callback);
}

export async function loginWithFacebookFirebase(): Promise<any> {
  const auth = getFirebaseAuth();
  if (!auth) {
    throw new Error("Facebook Sign-In indisponível porque o Firebase não está configurado.");
  }
  try {
    const provider = new FacebookAuthProvider();
    const cred = await signInWithPopup(auth, provider);
    return cred.user;
  } catch (error) {
    console.error("[Auth] Falha no Facebook Access:", error);
    throw error;
  }
}

export async function sendPasswordResetFirebase(email: string): Promise<void> {
  const auth = getFirebaseAuth();
  if (!auth) {
    throw new Error("O Firebase Auth não está configurado. Não é possível recuperar senha.");
  }
  try {
    await sendPasswordResetEmail(auth, email.trim());
  } catch (error) {
    console.error("[Auth] Falha no envio de e-mail de recuperação:", error);
    throw error;
  }
}

export async function deleteUserAccountFirebase(email: string): Promise<void> {
  const mailKey = email.toLowerCase().trim();
  if (!mailKey) return;

  // Limpa o armazenamento local do navegador imediatamente
  localStorage.removeItem("orbi_user_profile");
  localStorage.removeItem("orbi_extra_maps");
  localStorage.removeItem("star_map_dreams_v2");
  localStorage.removeItem("registered_accounts_data_local");
  
  const db = getFirestoreDB();
  if (db) {
    // Exclui subcoleções de mapas extras
    try {
      const extraRef = collection(db, "users", mailKey, "extraMaps");
      const extraSnap = await getDocs(extraRef);
      for (const d of extraSnap.docs) {
        await deleteDoc(doc(db, "users", mailKey, "extraMaps", d.id));
      }
    } catch (e) {
      console.warn("Falha ao deletar mapas extras no Firestore:", e);
    }

    // Exclui subcoleções de sonhos
    try {
      const dreamsRef = collection(db, "users", mailKey, "dreams");
      const dreamsSnap = await getDocs(dreamsRef);
      for (const d of dreamsSnap.docs) {
        await deleteDoc(doc(db, "users", mailKey, "dreams", d.id));
      }
    } catch (e) {
      console.warn("Falha ao deletar sonhos no Firestore:", e);
    }

    // Exclui o documento principal do usuário
    try {
      const userRef = doc(db, "users", mailKey);
      await deleteDoc(userRef);
    } catch (e) {
      console.warn("Falha ao deletar perfil principal no Firestore:", e);
      handleFirestoreError(e, OperationType.DELETE, `users/${mailKey}`);
    }
  }

  // Exclui a credencial de autenticação se houver login ativo no Firebase Auth
  const auth = getFirebaseAuth();
  if (auth && auth.currentUser) {
    try {
      await auth.currentUser.delete();
    } catch (e) {
      console.warn("Falha de deleção de usuário de auth direta (deslogando em vez disso):", e);
      await signOut(auth);
    }
  }
}


